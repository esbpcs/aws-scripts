#!/bin/bash
set -euo pipefail

# Cleanup function to remove the temporary file.
cleanup() {
    if [[ -n "${TEMP_SCRIPT:-}" && -f "$TEMP_SCRIPT" ]]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Cleaning up temporary file: $TEMP_SCRIPT" >&2
        rm -f "$TEMP_SCRIPT"
    fi
}
trap cleanup EXIT

# Usage: ./run-presigned.sh BUCKET_NAME SCRIPT_NAME [SCRIPT_PARAMS...]
if [ "$#" -lt 2 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Usage: $0 BUCKET_NAME SCRIPT_NAME [SCRIPT_PARAMS...]" >&2
    exit 1
fi

BUCKET_NAME="$1"
SCRIPT_NAME="$2"
shift 2 # Forward any additional parameters to the downloaded script.

# Check for required commands.
for cmd in aws curl bash; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Error: $cmd is not installed." >&2
        exit 1
    fi
done

# Prefer python3 if available, otherwise fallback to python.
if command -v python3 &>/dev/null; then
    PYTHON_CMD=python3
else
    PYTHON_CMD=python
fi
echo "$(date '+%Y-%m-%d %H:%M:%S') - Determined Python command: $PYTHON_CMD" >&2

# Function: install_pip
# Attempts to install pip using ensurepip; if that fails, falls back to downloading get-pip.py.
install_pip() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - pip not found. Attempting to install pip via ensurepip." >&2
    if ! $PYTHON_CMD -m ensurepip --upgrade; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - ensurepip failed. Downloading get-pip.py." >&2
        if curl --fail --silent https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py; then
            $PYTHON_CMD /tmp/get-pip.py --user
            rm -f /tmp/get-pip.py
        else
            echo "$(date '+%Y-%m-%d %H:%M:%S') - Error: Failed to download get-pip.py." >&2
            exit 1
        fi
    fi
}

# Hardcode the bucket's region.
BUCKET_REGION="ap-southeast-1"
echo "$(date '+%Y-%m-%d %H:%M:%S') - Bucket $BUCKET_NAME is in region: $BUCKET_REGION" >&2

echo "$(date '+%Y-%m-%d %H:%M:%S') - Generating pre-signed URL for s3://$BUCKET_NAME/$SCRIPT_NAME in region $BUCKET_REGION" >&2
PRESIGNED_URL=$(aws s3 presign "s3://$BUCKET_NAME/$SCRIPT_NAME" --expires-in 60 --region "$BUCKET_REGION")
if [ -z "$PRESIGNED_URL" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Error: Failed to generate a pre-signed URL." >&2
    exit 1
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') - Generated pre-signed URL (hidden): [REDACTED]" >&2

# Create a secure temporary file with a unique timestamp.
TEMP_SCRIPT=$(mktemp /tmp/execution.$(date +%Y%m%d%H%M%S).XXXXXX.sh)
chmod 700 "$TEMP_SCRIPT"
echo "$(date '+%Y-%m-%d %H:%M:%S') - Temporary file created: $TEMP_SCRIPT" >&2

# Download the main script securely.
if ! curl --fail --silent "$PRESIGNED_URL" -o "$TEMP_SCRIPT"; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Error: Failed to download the script from S3." >&2
    exit 1
fi

# Convert DOS/Windows line endings to Unix format.
sed -i 's/\r//g' "$TEMP_SCRIPT"
echo "$(date '+%Y-%m-%d %H:%M:%S') - Converted main script to Unix format." >&2

# Determine file extension.
ext="${SCRIPT_NAME##*.}"
echo "$(date '+%Y-%m-%d %H:%M:%S') - Detected script extension: $ext" >&2

# If the file extension is "py", check for a companion requirements file.
if [ "$ext" == "py" ]; then
    candidate_requirements=(
        "${SCRIPT_NAME}.Requirements.txt"
        "Requirements.txt"
        "../Requirements.txt"
        "../${SCRIPT_NAME}.Requirements.txt"
        "Dependencies/Requirements.txt"
        "Dependencies/${SCRIPT_NAME}.Requirements.txt"
        "dependencies/Requirements.txt"
        "dependencies/${SCRIPT_NAME}.Requirements.txt"
    )
    REQ_FOUND=""
    for candidate in "${candidate_requirements[@]}"; do
        # Use head-object to check if the file exists without requiring ListBucket permission.
        if aws s3api head-object --bucket "$BUCKET_NAME" --key "$candidate" --output text >/dev/null 2>&1; then
            REQ_FOUND="$candidate"
            echo "$(date '+%Y-%m-%d %H:%M:%S') - Found requirements file: $REQ_FOUND" >&2
            break
        fi
    done

    # If a requirements file is found, download and install dependencies.
    if [ -n "$REQ_FOUND" ]; then
        REQ_TEMP=$(mktemp /tmp/requirements.$(date +%Y%m%d%H%M%S).XXXXXX.txt)
        PRESIGNED_REQ=$(aws s3 presign "s3://$BUCKET_NAME/$REQ_FOUND" --expires-in 60 --region "$BUCKET_REGION")
        if [ -n "$PRESIGNED_REQ" ]; then
            if curl --fail --silent "$PRESIGNED_REQ" -o "$REQ_TEMP"; then
                # Compute MD5 hash of the requirements file.
                REQ_HASH=$(md5sum "$REQ_TEMP" | awk '{print $1}')
                MARKER_FILE="/tmp/${SCRIPT_NAME}.requirements.md5"
                SKIP_INSTALL=0
                if [ -f "$MARKER_FILE" ]; then
                    CACHED_HASH=$(cat "$MARKER_FILE")
                    if [ "$CACHED_HASH" == "$REQ_HASH" ]; then
                        SKIP_INSTALL=1
                        echo "$(date '+%Y-%m-%d %H:%M:%S') - Requirements file unchanged; skipping dependency installation." >&2
                    fi
                fi
                if [ "$SKIP_INSTALL" -eq 0 ]; then
                    echo "$(date '+%Y-%m-%d %H:%M:%S') - Installing Python dependencies from $REQ_FOUND." >&2
                    if ! $PYTHON_CMD -m pip --version &>/dev/null; then
                        install_pip
                    fi
                    $PYTHON_CMD -m pip install --upgrade pip
                    $PYTHON_CMD -m pip install -r "$REQ_TEMP"
                    # Save the current hash to the marker file.
                    echo "$REQ_HASH" >"$MARKER_FILE"
                fi
            else
                echo "$(date '+%Y-%m-%d %H:%M:%S') - Warning: Failed to download requirements file $REQ_FOUND." >&2
            fi
            rm -f "$REQ_TEMP"
        else
            echo "$(date '+%Y-%m-%d %H:%M:%S') - Warning: Could not generate pre-signed URL for requirements file." >&2
        fi
    else
        echo "$(date '+%Y-%m-%d %H:%M:%S') - No requirements file found; skipping dependency installation." >&2
    fi
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Detected non-Python script; skipping dependency installation." >&2
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') - Download successful. Executing the script... $*" >&2

# Execute the downloaded script with the proper interpreter.
if [ "$ext" == "py" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Executing with $PYTHON_CMD." >&2
    $PYTHON_CMD "$TEMP_SCRIPT" "$@"
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Executing with Bash." >&2
    bash "$TEMP_SCRIPT" "$@"
fi

# Temporary file will be cleaned up by the trap.
