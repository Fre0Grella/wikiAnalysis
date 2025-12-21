#!/usr/bin/env bash

set -e

# --- Default values ---
WIKI="enwiki"
DATE="20251201"  # Specific dump date
OUT_DIR="./dataset/sql_dumps"
PARSE_SCRIPT="./scripts/parse_categories.py"
PARSE_OUTPUT="./dataset/categories"
AUTO_PARSE=1

# --- Help message ---
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Download Wikimedia category SQL dumps and parse into hierarchical TSV.

OPTIONS:
  -w, --wiki WIKI         Wikipedia language code (default: enwiki)
  -d, --date DATE         Dump date YYYYMMDD (default: 20251201)
  -D, --dir DIRECTORY     Download directory (default: ./dataset/sql_dumps)
  -o, --output DIRECTORY  Parse output directory (default: ./output)
  -s, --parse-script PATH Path to parse_categories.py
  --no-parse              Download only, don't parse
  -n, --dry-run           Show what would happen
  -h, --help              Show this help message

EXAMPLES:
  $0                              # Download and parse latest enwiki
  $0 -w dewiki                    # German Wikipedia
  $0 -d 20240101 -D /mnt/dumps    # Specific dump date, custom dir
  $0 --no-parse                   # Download only

EOF
}

# --- Parse command-line arguments ---
DRY_RUN=0

while getopts ":w:d:D:o:s:nh-" opt; do
    case "$opt" in
        w) WIKI="$OPTARG" ;;
        d) DATE="$OPTARG" ;;
        D) OUT_DIR="$OPTARG" ;;
        o) PARSE_OUTPUT="$OPTARG" ;;
        s) PARSE_SCRIPT="$OPTARG" ;;
        n) DRY_RUN=1 ;;
        h) usage; exit 0 ;;
        -)
            case "${OPTARG}" in
                wiki=*) WIKI="${OPTARG#*=}" ;;
                date=*) DATE="${OPTARG#*=}" ;;
                dir=*) OUT_DIR="${OPTARG#*=}" ;;
                output=*) PARSE_OUTPUT="${OPTARG#*=}" ;;
                parse-script=*) PARSE_SCRIPT="${OPTARG#*=}" ;;
                no-parse) AUTO_PARSE=0 ;;
                dry-run) DRY_RUN=1 ;;
                help) usage; exit 0 ;;
                *) echo "Unknown option --${OPTARG}" >&2; exit 1 ;;
            esac;;
        *) echo "Unknown option: -$OPTARG" >&2; exit 1 ;;
    esac
done

# --- Main script logic ---
BASE_URL="https://dumps.wikimedia.org/${WIKI}/${DATE}"

# Files to download (smallest to largest for efficiency)
CATEGORYLINKS_FILE="${WIKI}-${DATE}-categorylinks.sql.gz"
CATEGORY_FILE="${WIKI}-${DATE}-category.sql.gz"

CATEGORYLINKS_URL="${BASE_URL}/${CATEGORYLINKS_FILE}"
CATEGORY_URL="${BASE_URL}/${CATEGORY_FILE}"

echo "══════════════════════════════════════════════════════════════"
echo " Wikimedia Category Downloader + Parser"
echo "══════════════════════════════════════════════════════════════"
echo ""
echo "Configuration:"
echo "  Wiki:              $WIKI"
echo "  Dump Date:         $DATE"
echo "  Download Dir:      $OUT_DIR"
echo "  Parse Output Dir:  $PARSE_OUTPUT"
echo "  Parse Script:      $PARSE_SCRIPT"
echo ""
echo "Files to download (smallest first):"
echo "  1. Categorylinks:   $CATEGORYLINKS_URL"
echo "  2. Category:        $CATEGORY_URL"
echo ""

if [ "$DRY_RUN" -eq 1 ]; then
    echo "Dry run complete. No files were downloaded."
    exit 0
fi

# Create directories
mkdir -p "$OUT_DIR"
mkdir -p "$PARSE_OUTPUT"

echo "Starting downloads..."
echo ""

# Download using aria2c if available (parallel + resume), otherwise curl
if command -v aria2c >/dev/null 2>&1; then
        echo "Using aria2c (parallel + resume support)..."
        aria2c \
            --dir="$OUT_DIR" \
            -Z \
            --max-concurrent-downloads=1 \
            --max-connection-per-server=2 \
            --min-split-size=20M \
            --split=2 \
            --continue=true \
            --max-tries=0 \
            --retry-wait=10 \
            --user-agent="WikimediaDumpDownloader/1.0 (contact: marco.galeri@studio.unibo.it)" \
            --allow-overwrite=true \
            "$CATEGORYLINKS_URL" \
            "$CATEGORY_URL"

else
    echo "Using curl (sequential download)..."
    echo "  [1/2] Downloading categorylinks (smaller)..."
    curl -f -L -# -o "$OUT_DIR/$CATEGORYLINKS_FILE" "$CATEGORYLINKS_URL"
    
    echo ""
    echo "  [2/2] Downloading category table (larger)..."
    curl -f -L -# -o "$OUT_DIR/$CATEGORY_FILE" "$CATEGORY_URL"
fi

echo ""
echo "✓ Downloads complete!"
echo ""
ls -lh "$OUT_DIR" | tail -n +2
echo ""

# Auto-parse if enabled
if [ "$AUTO_PARSE" -eq 1 ]; then
    echo "══════════════════════════════════════════════════════════════"
    echo " Parsing SQL dumps into category hierarchy..."
    echo "══════════════════════════════════════════════════════════════"
    echo ""
    
    if [ ! -f "$PARSE_SCRIPT" ]; then
        echo "Error: Parse script not found at $PARSE_SCRIPT"
        echo "Please ensure parse_categories.py is in the correct location."
        exit 1
    fi

    if ! command -v python >/dev/null 2>&1; then
        echo "Error: python3 not found"
        exit 1
    fi

    python "$PARSE_SCRIPT" \
        --category "$OUT_DIR/$CATEGORY_FILE" \
        --categorylinks "$OUT_DIR/$CATEGORYLINKS_FILE" \
        --output "$PARSE_OUTPUT"

    echo ""
    echo "══════════════════════════════════════════════════════════════"
    echo "✓ Parsing complete!"
    echo "══════════════════════════════════════════════════════════════"
    echo ""
    echo "Output file:"
    ls -lh "$PARSE_OUTPUT"/*.tsv 2>/dev/null || echo "  No TSV files generated"
else
    echo "Auto-parse disabled. Run parsing manually:"
    echo "  python3 $PARSE_SCRIPT \\"
    echo "    --category $OUT_DIR/$CATEGORY_FILE \\"
    echo "    --categorylinks $OUT_DIR/$CATEGORYLINKS_FILE \\"
    echo "    --output $PARSE_OUTPUT"
fi

echo ""
