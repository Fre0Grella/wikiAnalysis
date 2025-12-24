#!/usr/bin/env bash

set -e

# --- Default values ---
WIKI="enwiki"
DATE="20251201"  # Specific dump date
OUT_DIR="./dataset/sql_dumps"

# --- Help message ---
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Download Wikimedia category SQL dumps and parse into hierarchical TSV.

OPTIONS:
  -w, --wiki WIKI         Wikipedia language code (default: enwiki)
  -d, --date DATE         Dump date YYYYMMDD (default: 20251201)
  -D, --dir DIRECTORY     Download directory (default: ./dataset/sql_dumps)
  -n, --dry-run           Show what would happen
  -h, --help              Show this help message

EXAMPLES:
  $0                              # Download and parse latest enwiki
  $0 -w dewiki                    # German Wikipedia
  $0 -d 20240101 -D /mnt/dumps    # Specific dump date, custom dir

EOF
}

# --- Parse command-line arguments ---
DRY_RUN=0

while getopts ":w:d:D:o:s:nh-" opt; do
    case "$opt" in
        w) WIKI="$OPTARG" ;;
        d) DATE="$OPTARG" ;;
        D) OUT_DIR="$OPTARG" ;;
        n) DRY_RUN=1 ;;
        h) usage; exit 0 ;;
        -)
            case "${OPTARG}" in
                wiki=*) WIKI="${OPTARG#*=}" ;;
                date=*) DATE="${OPTARG#*=}" ;;
                dir=*) OUT_DIR="${OPTARG#*=}" ;;
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
LINKTARGET_FILE="${WIKI}-${DATE}-linktarget.sql.gz"
PAGES_FILE="${WIKI}-${DATE}-page.sql.gz"

CATEGORYLINKS_URL="${BASE_URL}/${CATEGORYLINKS_FILE}"
CATEGORY_URL="${BASE_URL}/${LINKTARGET_FILE}"
PAGES_URL="${BASE_URL}/${PAGES_FILE}"

echo "══════════════════════════════════════════════════════════════"
echo " Wikimedia Category Downloader"
echo "══════════════════════════════════════════════════════════════"
echo ""
echo "Configuration:"
echo "  Wiki:              $WIKI"
echo "  Dump Date:         $DATE"
echo "  Download Dir:      $OUT_DIR"
echo ""
echo "Files to download (smallest first):"
echo "  1. Categorylinks:   $CATEGORYLINKS_URL"
echo "  2. Category:        $CATEGORY_URL"
echo "  3. Pages:           $PAGES_URL"
echo ""

if [ "$DRY_RUN" -eq 1 ]; then
    echo "Dry run complete. No files were downloaded."
    exit 0
fi

# Create directories
mkdir -p "$OUT_DIR"
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
            "$CATEGORY_URL" \
            "$PAGES_URL"

else
    echo "Using curl (sequential download)..."
    echo "  [1/3] Downloading categorylinks ..."
    curl -f -L -# -o "$OUT_DIR/$CATEGORYLINKS_FILE" "$CATEGORYLINKS_URL"

    echo ""
    echo "  [2/3] Downloading link target table ..."
    curl -f -L -# -o "$OUT_DIR/$LINKTARGET_FILE" "$CATEGORY_URL"

    echo ""
    echo "  [3/3] Downloading pages table ..."
    curl -f -L -# -o "$OUT_DIR/$PAGES_FILE" "$PAGES_URL"
fi

echo ""
echo "✓ Downloads complete!"
echo ""
ls -lh "$OUT_DIR" | tail -n +2
echo ""
echo ""
