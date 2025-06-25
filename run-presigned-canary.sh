#!/usr/bin/env bash
###############################################################################
# Universal S3 Script Runner: Python, Bash, PowerShell
# - Secure/multi-language S3 download, auto interpreter
# - Python: venv, pip cache w/auto-prune, requirements deep-merge, pip-audit
# - Bash/PowerShell: Secure execution, batch/folder/archives
# - Full robust logging and entrypoint auto-detection
###############################################################################

set -o errexit -o nounset -o pipefail -o errtrace
IFS=$'\n\t'
umask 077
[[ "${TRACE:-0}" == "1" ]] && set -x

#---- Logging ----
log() {
  local ts offset
  ts=$(date +'%Y-%m-%dT%H:%M:%S')
  offset=$(date +'%:z' | sed 's/^+//')
  printf '%s%s %s\n' "$ts" "$offset" "$*"
}
log_info() { log "INFO:  $*"; }
log_error() { log "ERROR: $*" >&2; }

#---- Usage & argument parsing ----
usage() {
  cat <<EOF
Usage: $(basename "$0") -b BUCKET -s SCRIPT_KEY [options] [-- script-args]
  -b, --bucket       S3 bucket name
  -s, --script       S3 key/path (file, folder, or archive)
  -r, --region       AWS region (auto, if omitted)
  -e, --expires      Presigned URL TTL (1–604800, default=300)
  --entrypoint FILE  Entrypoint within archive/folder (user override)
  -h, --help         Show usage and exit
EOF
  exit 1
}

ENTRYPOINT=""
EXPIRES=300
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

#---- Setup: env, tempdir, utilities ----
PYTHON_CMD=$(command -v python3 || command -v python)
BASH_CMD=$(command -v bash)
PWSH_CMD=$(command -v pwsh 2>/dev/null || command -v powershell 2>/dev/null)
if [[ -z "$PWSH_CMD" ]] && [[ -x "/usr/bin/pwsh" ]]; then
  PWSH_CMD="/usr/bin/pwsh"
fi
for cmd in aws curl bash getopt unzip tar; do
  command -v "$cmd" &>/dev/null || {
    log_error "Utility required but missing: $cmd"
    exit 1
  }
done
BASE="${XDG_RUNTIME_DIR:-${TMPDIR:-/tmp}}"
SCRATCH_DIR=$(mktemp -d "$BASE/$(basename "$0").$$.XXXXXX")
trap 'rm -rf "$SCRATCH_DIR"' EXIT INT TERM ERR

#---- S3 download and type detection ----
EXT="${SCRIPT_KEY##*.}"
log_info "Generating presigned URL for S3 key"
PRESIGNED_URL=$(aws s3 presign "s3://$BUCKET/$SCRIPT_KEY" --region "${REGION:-us-east-1}" --expires-in "$EXPIRES")
if aws s3api head-object --bucket "$BUCKET" --key "$SCRIPT_KEY" &>/dev/null; then
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
    curl --fail --silent --show-error "$PRESIGNED_URL" -o "$ARCHIVE"
    unzip -q "$ARCHIVE" -d "$SCRATCH_DIR/project"
    PROJECT_TYPE="archive"
    PROJROOT="$SCRATCH_DIR/project"
    ;;
  tar | tgz | tar.gz)
    ARCHIVE="$SCRATCH_DIR/project.tar.gz"
    curl --fail --silent --show-error "$PRESIGNED_URL" -o "$ARCHIVE"
    mkdir -p "$SCRATCH_DIR/project"
    tar -xzf "$ARCHIVE" -C "$SCRATCH_DIR/project"
    PROJECT_TYPE="archive"
    PROJROOT="$SCRATCH_DIR/project"
    ;;
  *)
    TARGET="$SCRATCH_DIR/$(basename "$SCRIPT_KEY")"
    curl --fail --silent --show-error "$PRESIGNED_URL" -o "$TARGET"
    chmod +x "$TARGET"
    PROJECT_TYPE="single"
    PROJROOT="$SCRATCH_DIR"
    ;;
  esac
else
  aws s3 sync "s3://$BUCKET/$SCRIPT_KEY" "$SCRATCH_DIR/project"
  PROJECT_TYPE="folder"
  PROJROOT="$SCRATCH_DIR/project"
fi

#---- Entrypoint detection for folders/archives ----
detect_entrypoint() {
  if [[ -n "${ENTRYPOINT:-}" ]]; then
    ENTRY="$PROJROOT/$ENTRYPOINT"
  else
    entry_candidates=(
      main.py app.py __main__.py setup.py main.sh run.sh start.sh main.ps1 script.ps1
    )
    for fn in "${entry_candidates[@]}"; do
      CANDY=$(find "$PROJROOT" -maxdepth 1 -type f -iname "$fn" | head -n1)
      [[ -n "$CANDY" ]] && ENTRY="$CANDY" && break
    done
    [[ -z "${ENTRY:-}" ]] && ENTRY=$(find "$PROJROOT" -maxdepth 1 -type f $$ -iname "*.py" -o -iname "*.sh" -o -iname "*.ps1" $$ | sort | head -n 1)
  fi
  [[ -f "$ENTRY" ]] || {
    log_error "Entrypoint not found."
    exit 1
  }
  chmod +x "$ENTRY"
}
if [[ "$PROJECT_TYPE" == "single" ]]; then
  ENTRY="$TARGET"
  chmod +x "$ENTRY"
else
  detect_entrypoint
fi

# Fallback
if [[ -z "${ENTRY:-}" ]]; then
  for file in "$PROJROOT"/*; do
    # Only check regular files
    [[ -f "$file" ]] || continue
    first_line=$(head -n 1 "$file")
    case "$first_line" in
    '#!'*python*)
      ENTRY="$file"
      LANG=python
      break
      ;;
    '#!'*pwsh* | '#!'*powershell*)
      ENTRY="$file"
      LANG=powershell
      break
      ;;
    '#!'*sh*)
      ENTRY="$file"
      LANG=bash
      break
      ;;
    esac
  done
fi
if [[ -z "${ENTRY:-}" ]]; then
  log_error "Entrypoint not found by extension or shebang."
  exit 1
fi
chmod +x "$ENTRY"

#---- File type detection ----
ENTRY_NAME="$(basename "$ENTRY")"
if [[ "$ENTRY_NAME" == *.py ]]; then
  LANG=python
elif [[ "$ENTRY_NAME" == *.ps1 ]]; then
  LANG=powershell
elif [[ "$ENTRY_NAME" == *.sh ]]; then
  LANG=bash
else
  log_error "Unknown script extension for $ENTRY_NAME"
  exit 1
fi

#---- Universal execution logic ----
case "$LANG" in
python)
  #---- Persistent pip cache with size-prune ----
  export PIP_CACHE_DIR="${PIP_CACHE_DIR:-$HOME/.pip_cache}"
  mkdir -p "$PIP_CACHE_DIR"
  PIP_CACHE_MAX_MB="${PIP_CACHE_MAX_MB:-2048}"
  prune_pip_cache() {
    local cache_dir="$PIP_CACHE_DIR"
    local max_bytes=$((PIP_CACHE_MAX_MB * 1024 * 1024))
    if [[ -d "$cache_dir" ]]; then
      local du bytes
      du=$(du -sb "$cache_dir" | awk '{print $1}')
      bytes=$((du))
      if ((bytes > max_bytes)); then
        log_info "Pip cache exceeds ${PIP_CACHE_MAX_MB}MB, pruning"
        find "$cache_dir" -type f -printf '%A@ %p\n' | sort -n | while read atime file; do
          rm -f "$file"
          bytes=$(du -sb "$cache_dir" | awk '{print $1}')
          ((bytes <= max_bytes)) && break
        done
        log_info "Pip cache pruned to $(du -sh "$cache_dir" | awk '{print $1}')"
      fi
    fi
  }
  PY_VER=$("$PYTHON_CMD" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
  VENV="$SCRATCH_DIR/venv-py${PY_VER}"
  if [[ ! -d "$VENV" ]]; then
    log_info "Creating virtualenv at $VENV"
    "$PYTHON_CMD" -m venv "$VENV"
  fi
  PIP="$VENV/bin/pip"
  PY="$VENV/bin/python"
  "$PIP" install --upgrade pip
  # Requirements merge/install (deep)
  REQFILES=()
  while IFS= read -r -d '' file; do REQFILES+=("$file"); done < <(find "$PROJROOT" -type f -iname 'requirements.txt' -print0)
  if [[ "${#REQFILES[@]}" -gt 0 ]]; then
    log_info "Merging all requirements.txt files"
    cat "${REQFILES[@]}" | awk '/^[[:space:]]*($|#)/{next}{print $0}' | sort -fu >"$SCRATCH_DIR/requirements-all.txt"
    "$PIP" install -r "$SCRATCH_DIR/requirements-all.txt"
  else
    log_info "No requirements.txt found in $PROJROOT"
  fi
  #---- Dependency scan & pip install (all .py deep) ----
  log_info "Scanning for missing Python module imports (all project .py files, any depth)"
  declare -A MODULES
  mapfile -t PYSRC < <(find "$PROJROOT" -type f -name '*.py')
  for f in "${PYSRC[@]}"; do
    while read -r mod; do
      [[ -z "$mod" ]] && continue
      MODULES["$mod"]=1
    done < <(grep -E '^[[:space:]]*(import|from)[[:space:]]+' "$f" |
      sed -E 's/from +([^ ]+) .*/\1/;s/import +//g' |
      cut -d'.' -f1 | grep -v '^$')
  done
  #---- Up-to-date stdlib skip list ----
  read -r -d '' STDLIB_SKIP_LIST <<'STDEND'
abc
aifc
argparse
array
ast
asynchat
asyncio
asyncore
base64
bdb
binascii
bisect
builtins
bz2
cgi
cgitb
chunk
cmath
cmd
code
codecs
codeop
collections
colorsys
compileall
concurrent
configparser
contextlib
copy
copyreg
crypt
csv
ctypes
curses
dataclasses
datetime
dbm
decimal
difflib
dis
doctest
email
encodings
ensurepip
enum
errno
faulthandler
filecmp
fileinput
fnmatch
fractions
ftplib
functools
gc
getopt
getpass
gettext
glob
grp
gzip
hashlib
heapq
hmac
html
http
imaplib
imghdr
imp
importlib
inspect
io
ipaddress
itertools
json
keyword
lib2to3
linecache
locale
logging
lzma
mailbox
mailcap
marshal
math
mimetypes
mmap
modulefinder
multiprocessing
netrc
nntplib
numbers
opcode
operator
optparse
os
ossaudiodev
parser
pathlib
pdb
pickle
pickletools
pip
pkgutil
platform
plistlib
poplib
posix
pprint
profile
pstats
pty
pwd
py_compile
pyclbr
pydoc
queue
quopri
random
re
readline
reprlib
resource
rlcompleter
runpy
sched
secrets
select
selectors
shelve
shlex
shutil
signal
site
smtpd
smtplib
sndhdr
socket
socketserver
sqlite3
sre
sre_compile
sre_constants
sre_parse
ssl
stat
statistics
string
stringprep
struct
subprocess
sunau
symbol
symtable
sys
sysconfig
syslog
tabnanny
tarfile
telnetlib
tempfile
termios
test
textwrap
this
threading
time
timeit
tkinter
token
tokenize
trace
traceback
tracemalloc
tty
turtle
turtledemo
types
typing
unicodedata
unittest
urllib
uu
uuid
venv
warnings
wave
weakref
webbrowser
winreg
wsgiref
xdrlib
xml
xmlrpc
zipapp
zipfile
zipimport
zlib
__future__
__main__
STDEND

  is_stdlib() {
    local mod="$1"
    grep -qx "$mod" <<<"$STDLIB_SKIP_LIST"
  }

  install_module() {
    # Called only for missing modules not found in stdlib skip list
    local mod=$1 tried=""
    [[ "$mod" =~ ^[0-9]+$ ]] && return 0
    if "$PIP" show "$mod" &>/dev/null || "$PY" -c "import $mod" &>/dev/null; then
      return 0
    fi
    for pkg in "$mod" "${mod//_/-}"; do
      if "$PIP" install "$pkg"; then
        log_info "Installed $pkg for missing module '$mod'"
        return 0
      fi
      tried+=" $pkg"
    done
    log_error "Unable to install: $tried"
    return 1
  }
  for m in "${!MODULES[@]}"; do
    [[ "$m" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]] || continue
    is_stdlib "$m" && continue
    install_module "$m"
  done
  #---- pip-audit vulnerability scan ----
  log_info "Performing pip-audit security scan"
  if "$PIP" install --disable-pip-version-check --quiet pip-audit; then
    AUDIT_OUT="$SCRATCH_DIR/pip-audit-report.txt"
    if "$VENV/bin/pip-audit" -r "$AUDIT_OUT"; then
      log_info "pip-audit: no known vulnerabilities found"
    else
      log_error "pip-audit found vulnerabilities (see $AUDIT_OUT)"
      cat "$AUDIT_OUT"
      # exit 13 # Uncomment to fail on vulnerabilities
    fi
  else
    log_error "pip-audit install failed; skipping security audit"
  fi
  [[ "$PROJECT_TYPE" != "single" ]] && export PYTHONPATH="$PROJROOT:$PYTHONPATH" && cd "$PROJROOT"
  log_info "Executing Python: $ENTRY $*"
  "$PY" "$ENTRY" "$@"
  STATUS=$?
  log_info "Entrypoint exited with code $STATUS"
  prune_pip_cache
  exit $STATUS
  ;;
bash)
  [[ "$PROJECT_TYPE" != "single" ]] && cd "$PROJROOT"
  log_info "Executing Bash: $ENTRY $*"
  "$BASH_CMD" "$ENTRY" "$@"
  STATUS=$?
  log_info "Entrypoint exited with code $STATUS"
  exit $STATUS
  ;;
powershell)
  [[ -z "$PWSH_CMD" ]] && {
    log_error "No pwsh or powershell interpreter found!"
    exit 1
  }
  [[ "$PROJECT_TYPE" != "single" ]] && cd "$PROJROOT"
  log_info "Executing PowerShell: $ENTRY $*"
  "$PWSH_CMD" -NoLogo -NoProfile -ExecutionPolicy Bypass -File "$ENTRY" "$@"
  STATUS=$?
  log_info "Entrypoint exited with code $STATUS"
  exit $STATUS
  ;;
*)
  log_error "Unknown language: $LANG"
  exit 1
  ;;
esac
