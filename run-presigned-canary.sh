#!/usr/bin/env bash
###############################################################################
#
# Universal S3 Script Runner (Canary Version)
#
# DESCRIPTION:
#   Securely downloads and executes a script bundle from a private S3 bucket.
#   It automatically discovers dependencies, handles configurations, and
#   copies any generated report files back to the user's directory.
#
# USAGE:
#   ./run-presigned-canary.sh -b <BUCKET> -s <SCRIPT_KEY> [options] [-- script-args]
#
###############################################################################

# --- Script Configuration ---
set -o errexit -o nounset -o pipefail -o errtrace
IFS=$'\n\t'
umask 077
[[ "${TRACE:-0}" == "1" ]] && set -x

# --- Logging Functions ---
log() {
  local ts offset
  ts=$(date +'%Y-%m-%dT%H:%M:%S')
  offset=$(date +'%:z' | sed 's/^+//')
  printf '%s%s %s\n' "$ts" "$offset" "$*"
}
log_info() { log "INFO:  $*"; }
log_error() { log "ERROR: $*" >&2; }

# --- Argument Parsing ---
usage() {
  cat <<EOF
Usage: $(basename "$0") -b BUCKET -s SCRIPT_KEY [options] [-- script-args]
  -b, --bucket           S3 bucket name (required)
  -s, --script           S3 key for the script/bundle (required)
  -c, --config-file      Configuration file name to download (default: config.json)
  -e, --expires          Presigned URL TTL in seconds (1–604800, default: 300)
  --entrypoint FILE      Specify the entrypoint within an archive/folder
  -h, --help             Show this usage information and exit
EOF
  exit 1
}

# Initialize variables with defaults
BUCKET=""
SCRIPT_KEY=""
CONFIG_FILE="config.json"
EXPIRES=300
ENTRYPOINT=""
SCRIPT_ARGS=()

# Parse command-line options
while [[ "$#" -gt 0 ]]; do
  case "$1" in
  -b | --bucket)
    BUCKET="$2"
    shift 2
    ;;
  -s | --script)
    SCRIPT_KEY="$2"
    shift 2
    ;;
  -c | --config-file)
    CONFIG_FILE="$2"
    shift 2
    ;;
  -e | --expires)
    EXPIRES="$2"
    shift 2
    ;;
  --entrypoint)
    ENTRYPOINT="$2"
    shift 2
    ;;
  -h | --help) usage ;;
  --)
    shift
    SCRIPT_ARGS=("$@")
    break
    ;;
  *)
    log_error "Unknown option: $1"
    usage
    ;;
  esac
done
: "${BUCKET:?Missing -b|--bucket}"
: "${SCRIPT_KEY:?Missing -s|--script}"

# --- Environment Setup ---
for cmd in aws curl bash unzip tar md5sum dirname find head; do
  command -v "$cmd" &>/dev/null || {
    log_error "Utility required but missing: $cmd"
    exit 1
  }
done
ORIGINAL_PWD=$(pwd)
SCRATCH_DIR=$(mktemp -d "/tmp/$(basename "$0").$$.XXXXXX")
trap 'rm -rf "$SCRATCH_DIR"' EXIT INT TERM ERR
log_info "Created temporary directory at $SCRATCH_DIR"
PYTHON_CMD=$(command -v python3 || command -v python)

# --- Automatic Bucket Region Detection ---
log_info "Detecting region for bucket '$BUCKET'..."
BUCKET_REGION=$(aws s3api get-bucket-location --bucket "$BUCKET" --output text 2>/dev/null)
if [[ -z "$BUCKET_REGION" || "$BUCKET_REGION" == "None" ]]; then
  BUCKET_REGION="us-east-1"
fi
log_info "Bucket region detected: $BUCKET_REGION"

# --- S3 Object Download and Extraction ---
log_info "Generating presigned URL for s3://${BUCKET}/${SCRIPT_KEY}"
PRESIGNED_URL=$(aws s3 presign "s3://${BUCKET}/${SCRIPT_KEY}" --region "$BUCKET_REGION" --expires-in "$EXPIRES")

if aws s3api head-object --bucket "$BUCKET" --key "$SCRIPT_KEY" --region "$BUCKET_REGION" &>/dev/null; then
  log_info "Detected S3 key as a single object."
  TARGET="$SCRATCH_DIR/$(basename "$SCRIPT_KEY")"

  if ! curl --fail --silent --show-error "$PRESIGNED_URL" -o "$TARGET"; then
    log_error "curl command failed to download from the presigned URL."
    exit 1
  fi

  if [[ "$(head -n 1 "$TARGET")" == *"<?xml"* ]]; then
    log_error "Download failed. The downloaded file is an S3 error document, not the script."
    log_error "This confirms an issue with permissions or URL generation. Please verify IAM and Bucket Policies."
    exit 1
  fi

  chmod +x "$TARGET"
  PROJECT_TYPE="single"
  PROJROOT="$SCRATCH_DIR"
else
  log_info "S3 key is not a single object, treating as a folder prefix."
  PROJROOT="$SCRATCH_DIR/project"
  mkdir -p "$PROJROOT"
  aws s3 sync "s3://${BUCKET}/${SCRIPT_KEY}" "$PROJROOT" --quiet --region "$BUCKET_REGION"
  PROJECT_TYPE="folder"
fi
log_info "Download and extraction complete."

# --- Configuration File Handling ---
if [[ -n "$CONFIG_FILE" ]]; then
  SCRIPT_DIR=$(dirname "$SCRIPT_KEY")
  CONFIG_KEY="$CONFIG_FILE"
  if [[ "$SCRIPT_DIR" != "." ]]; then
    CONFIG_KEY="$SCRIPT_DIR/$CONFIG_FILE"
  fi

  log_info "Searching for config file dependency at s3://${BUCKET}/${CONFIG_KEY}"
  if aws s3api head-object --bucket "$BUCKET" --key "$CONFIG_KEY" --region "$BUCKET_REGION" &>/dev/null; then
    log_info "Found specified config file '$CONFIG_FILE'. Downloading..."
    CONFIG_URL=$(aws s3 presign "s3://${BUCKET}/${CONFIG_KEY}" --region "$BUCKET_REGION" --expires-in "$EXPIRES")
    curl --fail --silent --show-error "$CONFIG_URL" -o "$PROJROOT/$CONFIG_FILE"
  else
    log_info "Specified config file '$CONFIG_FILE' not found at '$CONFIG_KEY'."
  fi
fi

# --- Entrypoint & Language Detection ---
detect_entrypoint() {
  if [[ -n "${ENTRYPOINT:-}" ]]; then
    ENTRY="$PROJROOT/$ENTRYPOINT"
  else
    entry_candidates=(main.py app.py __main__.py setup.py main.sh run.sh start.sh main.ps1 script.ps1)
    for fn in "${entry_candidates[@]}"; do
      CANDIDATE=$(find "$PROJROOT" -maxdepth 1 -type f -iname "$fn" | head -n1)
      if [[ -n "$CANDIDATE" ]]; then
        ENTRY="$CANDIDATE"
        break
      fi
    done
    if [[ -z "${ENTRY:-}" ]]; then
      ENTRY=$(find "$PROJROOT" -maxdepth 1 -type f \( -iname "*.py" -o -iname "*.sh" -o -iname "*.ps1" \) | sort | head -n 1)
    fi
  fi
  if [[ -f "${ENTRY:-}" ]]; then
    log_info "Detected entrypoint: $(basename "$ENTRY")"
    chmod +x "$ENTRY"
  else
    log_error "Could not detect a valid script entrypoint in the project."
    exit 1
  fi
}

if [[ "$PROJECT_TYPE" == "single" ]]; then
  ENTRY="$TARGET"
else
  detect_entrypoint
fi

ENTRY_NAME="$(basename "$ENTRY")"
if [[ "$ENTRY_NAME" == *.py ]]; then
  LANG=python
elif [[ "$ENTRY_NAME" == *.ps1 ]]; then
  LANG=powershell
elif [[ "$ENTRY_NAME" == *.sh ]]; then
  LANG=bash
else
  case "$(head -n 1 "$ENTRY")" in
  '#!'*/python*) LANG=python ;;
  '#!'*/pwsh* | '#!'*/powershell*) LANG=powershell ;;
  '#!'*/bash* | '#!'*/sh*) LANG=bash ;;
  *)
    log_error "Cannot determine language for entrypoint: $ENTRY_NAME"
    exit 1
    ;;
  esac
fi
log_info "Detected script language: $LANG"

# --- Script Execution Logic ---
case "$LANG" in
python)
  VENV="$SCRATCH_DIR/venv"
  log_info "Creating Python virtual environment at $VENV"
  "$PYTHON_CMD" -m venv "$VENV"
  PIP="$VENV/bin/pip"
  PY="$VENV/bin/python"
  "$PIP" install --upgrade --quiet pip

  REQUIREMENTS_FILE_TO_INSTALL=""
  if [[ "$PROJECT_TYPE" == "single" ]]; then
    SCRIPT_S3_DIR=$(dirname "$SCRIPT_KEY")
    CANDIDATE_PATHS=(
      "Dependencies/requirements.txt" "Dependencies/Requirements.txt"
      "dependencies/requirements.txt" "dependencies/Requirements.txt" "Requirements.txt"
    )

    for path_suffix in "${CANDIDATE_PATHS[@]}"; do
      REQ_S3_KEY="$path_suffix"
      if [[ "$SCRIPT_S3_DIR" != "." && "$path_suffix" != "Requirements.txt" ]]; then
        REQ_S3_KEY="$SCRIPT_S3_DIR/$path_suffix"
      fi

      if aws s3api head-object --bucket "$BUCKET" --key "$REQ_S3_KEY" --region "$BUCKET_REGION" &>/dev/null; then
        LOCAL_REQ_PATH="$PROJROOT/$path_suffix"
        mkdir -p "$(dirname "$LOCAL_REQ_PATH")"

        log_info "Found requirements file at S3 path. Downloading..."
        REQ_URL=$(aws s3 presign "s3://${BUCKET}/${REQ_S3_KEY}" --region "$BUCKET_REGION" --expires-in "$EXPIRES")

        if ! curl --fail --silent --show-error "$REQ_URL" -o "$LOCAL_REQ_PATH"; then
          log_error "Failed to download requirements file from S3. Aborting."
          exit 1
        fi

        REQUIREMENTS_FILE_TO_INSTALL="$LOCAL_REQ_PATH"
        break
      fi
    done
  else
    REQUIREMENTS_FILE_TO_INSTALL=$(find "$PROJROOT" -type f -iname 'requirements.txt' | head -n 1)
  fi

  if [[ -n "$REQUIREMENTS_FILE_TO_INSTALL" && -f "$REQUIREMENTS_FILE_TO_INSTALL" ]]; then
    log_info "Installing dependencies from: ${REQUIREMENTS_FILE_TO_INSTALL}"
    "$PIP" install -r "$REQUIREMENTS_FILE_TO_INSTALL" --quiet
    log_info "Dependency installation complete."
  else
    log_info "No requirements.txt file found. Skipping dependency installation."
  fi

  cd "$PROJROOT"
  log_info "Executing Python script: $(basename "$ENTRY") ${SCRIPT_ARGS[*]}"
  "$PY" "$(basename "$ENTRY")" "${SCRIPT_ARGS[@]}"
  STATUS=$?
  log_info "Python script exited with status code $STATUS"
  ;;

bash | sh)
  cd "$PROJROOT"
  log_info "Executing Shell script: $(basename "$ENTRY") ${SCRIPT_ARGS[*]}"
  bash "$(basename "$ENTRY")" "${SCRIPT_ARGS[@]}"
  STATUS=$?
  log_info "Shell script exited with status code $STATUS"
  ;;

powershell)
  PWSH_CMD=$(command -v pwsh 2>/dev/null)
  if [[ -z "$PWSH_CMD" ]]; then
    log_error "PowerShell interpreter (pwsh) not found."
    exit 1
  fi
  cd "$PROJROOT"
  log_info "Executing PowerShell script: $(basename "$ENTRY") ${SCRIPT_ARGS[@]}"
  "$PWSH_CMD" -NoLogo -NoProfile -ExecutionPolicy Bypass -File "$(basename "$ENTRY")" "${SCRIPT_ARGS[@]}"
  STATUS=$?
  log_info "PowerShell script exited with status code $STATUS"
  ;;

*)
  log_error "Unsupported script language detected: $LANG"
  exit 1
  ;;
esac

# --- Artifact Handling ---
log_info "Searching for report files (*.xlsx, *.html, *.csv, *.zip) in temporary directory..."
find "$SCRATCH_DIR" -type f \( -iname "*.xlsx" -o -iname "*.html" -o -iname "*.csv" -o -iname "*.zip" \) -print0 | while IFS= read -r -d $'\0' file; do
  log_info "Found report: $(basename "$file"). Copying to $ORIGINAL_PWD"
  mv "$file" "$ORIGINAL_PWD/"
done

# The trap will now clean up the rest of the temporary files.
exit $STATUS
