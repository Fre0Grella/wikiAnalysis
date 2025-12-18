#!/usr/bin/env bash

set -e

# --- Default values ---
WIKI="enwiki"
OUT_DIR="./dataset/sql_dumps"
# Updated default path to match typical flat structure, change if needed
PARSE_SCRIPT="./parse_sql.py"
PARSE_OUTPUT="./dataset/categories"
AUTO_PARSE=1

# --- Help message ---
usage() {
    cat << EOF
Usage: $0 [OPTIONS]
Download Wikimedia SQL dumps and automatically parse them into category tree.

OPTIONS:
  -w, --wiki WIKI       Wikipedia language code (default: enwiki)
  -d, --dir DIRECTORY   Download directory (default: ./dataset/sql_dumps)
  -o, --output DIRECTORY Parse output directory (default: ./output)
  -s, --parse-script PATH Path to parse_sql.py
  --no-parse            Download only, don't parse
  -n, --dry-run         Show what would happen
  -h, --help            Show this help message
EOF
}

# --- Parse command-line arguments ---
DRY_RUN=0

while getopts ":w:d:o:s:nh-" opt; do
    case "$opt" in
        w) WIKI="$OPTARG" ;;
        d) OUT_DIR="$OPTARG" ;;
        o) PARSE_OUTPUT="$OPTARG" ;;
        s) PARSE_SCRIPT="$OPTARG" ;;
        n) DRY_RUN=1 ;;
        h) usage; exit 0 ;;
        -)
            case "${OPTARG}" in
                wiki=*) WIKI="${OPTARG#*=}" ;;
                dir=*) OUT_DIR="${OPTARG#*=}" ;;
                output=*) PARSE_OUTPUT="${OPTARG#*=}" ;;
                parse-script=*) PARSE_SCRIPT="${OPTARG#*=}" ;;
                no-parse) AUTO_PARSE=0 ;;
                dry-run) DRY_RUN=1 ;;
                help) usage; exit 0 ;;
                *) echo "Unknown option --${OPTARG}" >&2; usage ;;
            esac;;
        *) echo "Unknown option: -$OPTARG" >&2; usage ;;
    esac
done

# --- Main script logic ---
BASE_URL="https://dumps.wikimedia.org/${WIKI}/latest"
PAGE_FILE="${WIKI}-latest-page.sql.gz"
CATLINKS_FILE="${WIKI}-latest-categorylinks.sql.gz"
PAGE_URL="${BASE_URL}/${PAGE_FILE}"
CATLINKS_URL="${BASE_URL}/${CATLINKS_FILE}"

echo "══════════════════════════════════════════════════════════════"
echo " Wikimedia SQL Downloader + Parser"
echo "══════════════════════════════════════════════════════════════"
echo ""
echo "Configuration:"
echo "  Wiki:             $WIKI"
echo "  SQL Download Dir: $OUT_DIR"
echo "  Parse Output Dir: $PARSE_OUTPUT"
echo "  Parse Script:     $PARSE_SCRIPT"
echo ""

if [ "$DRY_RUN" -eq 1 ]; then
    echo "Dry run complete. No files were downloaded."
    exit 0
fi

# Create directories
mkdir -p "$OUT_DIR"
mkdir -p "$PARSE_OUTPUT"

echo "Starting download..."
echo ""

# Download using aria2c if available, otherwise curl
if command -v aria2c >/dev/null 2>&1; then
    echo "Using aria2c (supports resume)..."
    aria2c \
    --dir="$OUT_DIR" \
    --continue=true \
    --max-connection-per-server=1 \
    --split=1 \
    --max-concurrent-downloads=1 \
    --user-agent="Wikimedia-SQL-Downloader/1.0" \
    "$PAGE_URL" \
    "$CATLINKS_URL"
else
    echo "Using curl (no resume support)..."
    echo "Downloading page table..."
    curl -f -L -# -o "$OUT_DIR/$PAGE_FILE" "$PAGE_URL"

    echo ""
    echo "Downloading category links..."
    curl -f -L -# -o "$OUT_DIR/$CATLINKS_FILE" "$CATLINKS_URL"
fi

echo ""
echo "✓ Downloads complete!"
ls -lh "$OUT_DIR" | tail -n +2

# Auto-parse
if [ "$AUTO_PARSE" -eq 1 ]; then
    echo ""
    echo "══════════════════════════════════════════════════════════════"
    echo " Auto-parsing SQL dumps..."
    echo "══════════════════════════════════════════════════════════════"

    if [ ! -f "$PARSE_SCRIPT" ]; then
        echo " Error: Parse script not found at $PARSE_SCRIPT"
        echo " Please ensure parse_sql.py is in the correct location."
        exit 1
    fi

    if ! command -v py >/dev/null 2>&1; then
        echo " Error: python3 not found"
        exit 1
    fi

    echo ""
    python3 "$PARSE_SCRIPT" \
        --page "$OUT_DIR/$PAGE_FILE" \
        --categorylinks "$OUT_DIR/$CATLINKS_FILE" \
        --output "$PARSE_OUTPUT"

    echo ""
    echo "✓ Parsing complete!"
else
    echo "Auto-parse disabled."
fi
