#!/usr/bin/env bash
###############################################################################
#
# Universal S3 Script Runner
#
# DESCRIPTION:
#   Securely downloads and executes a script (Python, Bash, or PowerShell)
#   from a private S3 bucket using a presigned URL. It automatically detects
#   the language, sets up a clean environment, and manages dependencies.
#
# FEATURES:
#   - Python: Manages a virtual environment, finds and installs all
#     `requirements.txt` files, and runs a `pip-audit` security scan.
#   - Execution: Auto-detects the script entrypoint for archives/folders.
#   - Logging: Provides detailed and timestamped logs for all operations.
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
  -b, --bucket       S3 bucket name
  -s, --script       S3 key/path (file, folder, or archive)
  -r, --region       AWS region (auto-detected if omitted)
  -e, --expires      Presigned URL TTL in seconds (1–604800, default: 300)
  --entrypoint FILE  Specify the entrypoint within an archive/folder
  -h, --help         Show this usage information and exit
EOF
  exit 1
}

# Initialize variables
ENTRYPOINT=""
EXPIRES=300

# Parse command-line options using getopt
ARGS=$(getopt -o b:s:r:e:h --long bucket:,script:,region:,expires:,help,entrypoint: -- "$@") || usage
eval set -- "$ARGS"
while true; do
  case "$1" in
  -b | --bucket)
    BUCKET=$2
    shift 2
    ;;
  -s | --script)
    SCRIPT_KEY=$2
    shift 2
    ;;
  -r | --region)
    REGION=$2
    shift 2
    ;;
  -e | --expires)
    EXPIRES=$2
    shift 2
    ;;
  --entrypoint)
    ENTRYPOINT=$2
    shift 2
    ;;
  -h | --help)
    usage
    shift
    ;;
  --)
    shift
    break
    ;;
  *) usage ;;
  esac
done
: "${BUCKET:?Missing -b|--bucket}"
: "${SCRIPT_KEY:?Missing -s|--script}"

# --- Environment Setup ---
# Verify required command-line utilities are installed
for cmd in aws curl bash getopt unzip tar; do
  command -v "$cmd" &>/dev/null || {
    log_error "Utility required but missing: $cmd"
    exit 1
  }
done

# Set up a secure, temporary directory for all operations
BASE="${XDG_RUNTIME_DIR:-${TMPDIR:-/tmp}}"
SCRATCH_DIR=$(mktemp -d "$BASE/$(basename "$0").$$.XXXXXX")
trap 'rm -rf "$SCRATCH_DIR"' EXIT INT TERM ERR
log_info "Created temporary directory at $SCRATCH_DIR"

# Determine which interpreters are available
PYTHON_CMD=$(command -v python3 || command -v python)
BASH_CMD=$(command -v bash)
PWSH_CMD=$(command -v pwsh 2>/dev/null || command -v powershell 2>/dev/null)
if [[ -z "$PWSH_CMD" ]] && [[ -x "/usr/bin/pwsh" ]]; then
  PWSH_CMD="/usr/bin/pwsh"
fi

# --- S3 Object Download and Extraction ---
EXT="${SCRIPT_KEY##*.}"
log_info "Generating presigned URL for s3://${BUCKET}/${SCRIPT_KEY}"
PRESIGNED_URL=$(aws s3 presign "s3://${BUCKET}/${SCRIPT_KEY}" --region "${REGION:-us-east-1}" --expires-in "$EXPIRES")

# Determine if the S3 key is a single file, archive, or folder, then download
if aws s3api head-object --bucket "$BUCKET" --key "$SCRIPT_KEY" &>/dev/null; then
  log_info "Detected S3 key as a single object."
  case "$EXT" in
  py | sh | ps1)
    TARGET="$SCRATCH_DIR/$(basename "$SCRIPT_KEY")"
    curl --fail --silent --show-error "$PRESIGNED_URL" -o "$TARGET"
    chmod +x "$TARGET"
    PROJECT_TYPE="single"
    PROJROOT="$SCRATCH_DIR"
    ;;
  zip)
    ARCHIVE="$SCRATCH_DIR/project.zip"
    log_info "Downloading zip archive..."
    curl --fail --silent --show-error "$PRESIGNED_URL" -o "$ARCHIVE"
    unzip -q "$ARCHIVE" -d "$SCRATCH_DIR/project"
    PROJECT_TYPE="archive"
    PROJROOT="$SCRATCH_DIR/project"
    ;;
  tar | tgz | tar.gz)
    ARCHIVE="$SCRATCH_DIR/project.tar.gz"
    log_info "Downloading tarball archive..."
    curl --fail --silent --show-error "$PRESIGNED_URL" -o "$ARCHIVE"
    mkdir -p "$SCRATCH_DIR/project"
    tar -xzf "$ARCHIVE" -C "$SCRATCH_DIR/project"
    PROJECT_TYPE="archive"
    PROJROOT="$SCRATCH_DIR/project"
    ;;
  *)
    log_error "Unsupported file extension for single object: $EXT"
    exit 1
    ;;
  esac
else
  log_info "S3 key is not a single object, treating as a folder prefix."
  aws s3 sync "s3://${BUCKET}/${SCRIPT_KEY}" "$SCRATCH_DIR/project"
  PROJECT_TYPE="folder"
  PROJROOT="$SCRATCH_DIR/project"
fi
log_info "Download and extraction complete."

# --- Entrypoint Detection ---
# For archives or folders, find the primary script to execute.
detect_entrypoint() {
  # If an entrypoint is explicitly provided, use it.
  if [[ -n "${ENTRYPOINT:-}" ]]; then
    ENTRY="$PROJROOT/$ENTRYPOINT"
    log_info "Using specified entrypoint: $ENTRYPOINT"
  else
    # Otherwise, search for common entrypoint filenames.
    log_info "No entrypoint specified, searching for a default..."
    entry_candidates=(main.py app.py __main__.py setup.py main.sh run.sh start.sh main.ps1 script.ps1)
    for fn in "${entry_candidates[@]}"; do
      CANDY=$(find "$PROJROOT" -maxdepth 1 -type f -iname "$fn" | head -n1)
      if [[ -n "$CANDY" ]]; then
        ENTRY="$CANDY"
        break
      fi
    done
    # If no common entrypoint is found, take the first script file discovered.
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

# --- Language Detection ---
# Determine the script's language based on its file extension.
ENTRY_NAME="$(basename "$ENTRY")"
if [[ "$ENTRY_NAME" == *.py ]]; then
  LANG=python
elif [[ "$ENTRY_NAME" == *.ps1 ]]; then
  LANG=powershell
elif [[ "$ENTRY_NAME" == *.sh ]]; then
  LANG=bash
else
  # As a last resort, check the shebang line.
  first_line=$(head -n 1 "$ENTRY")
  case "$first_line" in
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
  # --- Python Environment Setup ---
  # Create and manage a persistent pip cache with auto-pruning.
  export PIP_CACHE_DIR="${PIP_CACHE_DIR:-$HOME/.pip_cache}"
  mkdir -p "$PIP_CACHE_DIR"
  PIP_CACHE_MAX_MB="${PIP_CACHE_MAX_MB:-2048}"
  prune_pip_cache() {
    local cache_dir="$PIP_CACHE_DIR"
    local max_bytes=$((PIP_CACHE_MAX_MB * 1024 * 1024))
    if [[ -d "$cache_dir" ]]; then
      local current_bytes
      current_bytes=$(du -sb "$cache_dir" | awk '{print $1}')
      if ((current_bytes > max_bytes)); then
        log_info "Pip cache size ($(du -sh "$cache_dir" | awk '{print $1}')) exceeds ${PIP_CACHE_MAX_MB}MB, pruning..."
        # Find and delete oldest files until the cache is under the limit
        find "$cache_dir" -type f -printf '%A@ %p\n' | sort -n | while read -r atime file; do
          rm -f "$file"
          current_bytes=$(du -sb "$cache_dir" | awk '{print $1}')
          ((current_bytes <= max_bytes)) && break
        done
        log_info "Pip cache pruned to $(du -sh "$cache_dir" | awk '{print $1}')"
      fi
    fi
  }

  # Set up a temporary Python virtual environment.
  PY_VER=$("$PYTHON_CMD" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
  VENV="$SCRATCH_DIR/venv-py${PY_VER}"
  log_info "Creating Python $PY_VER virtual environment at $VENV"
  "$PYTHON_CMD" -m venv "$VENV"
  PIP="$VENV/bin/pip"
  PY="$VENV/bin/python"
  "$PIP" install --upgrade pip

  # --- Dependency Installation ---
  # Find all requirements.txt files, merge them, and install the dependencies.
  MERGED_REQS_FILE="$SCRATCH_DIR/requirements-all.txt"
  REQFILES=()
  while IFS= read -r -d '' file; do REQFILES+=("$file"); done < <(find "$PROJROOT" -path "$VENV" -prune -o -type f -iname 'requirements.txt' -print0)
  if [[ "${#REQFILES[@]}" -gt 0 ]]; then
    log_info "Found ${#REQFILES[@]} requirements.txt file(s). Merging and installing dependencies."
    cat "${REQFILES[@]}" | awk '/^[[:space:]]*($|#)/{next}{print $0}' | sort -fu >"$MERGED_REQS_FILE"
    "$PIP" install -r "$MERGED_REQS_FILE"
  else
    log_info "No requirements.txt found. Creating an empty file for audit purposes."
    touch "$MERGED_REQS_FILE"
  fi

  # --- Security Scan ---
  log_info "Performing security vulnerability scan with pip-audit"
  if "$PIP" install --disable-pip-version-check --quiet pip-audit; then
    AUDIT_OUT="$SCRATCH_DIR/pip-audit-report.txt"
    if "$VENV/bin/pip-audit" -r "$MERGED_REQS_FILE" --output "$AUDIT_OUT"; then
      log_info "pip-audit: No known vulnerabilities found."
    else
      log_error "pip-audit found vulnerabilities! See details below:"
      cat "$AUDIT_OUT" >&2
      # To fail the script on vulnerability detection, uncomment the next line
      # exit 13
    fi
  else
    log_error "Could not install pip-audit; skipping security scan."
  fi

  # --- Execution ---
  if [[ "$PROJECT_TYPE" != "single" ]]; then
    log_info "Adding project root to PYTHONPATH and changing directory."
    export PYTHONPATH="$PROJROOT:$PYTHONPATH"
    cd "$PROJROOT"
  fi
  log_info "Executing Python script: $ENTRY $*"
  "$PY" "$ENTRY" "$@"
  STATUS=$?
  log_info "Script exited with status code $STATUS"
  prune_pip_cache
  exit $STATUS
  ;;
bash)
  # --- Execution ---
  [[ "$PROJECT_TYPE" != "single" ]] && cd "$PROJROOT"
  log_info "Executing Bash script: $ENTRY $*"
  "$BASH_CMD" "$ENTRY" "$@"
  STATUS=$?
  log_info "Script exited with status code $STATUS"
  exit $STATUS
  ;;
powershell)
  # --- Execution ---
  if [[ -z "$PWSH_CMD" ]]; then
    log_error "PowerShell interpreter (pwsh) not found."
    exit 1
  fi
  [[ "$PROJECT_TYPE" != "single" ]] && cd "$PROJROOT"
  log_info "Executing PowerShell script: $ENTRY $*"
  "$PWSH_CMD" -NoLogo -NoProfile -ExecutionPolicy Bypass -File "$ENTRY" "$@"
  STATUS=$?
  log_info "Script exited with status code $STATUS"
  exit $STATUS
  ;;
*)
  log_error "Unsupported script language: $LANG"
  exit 1
  ;;
esac
