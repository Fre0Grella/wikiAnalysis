#!/bin/sh

# Script to download Wikimedia history dumps for a specified wiki and date range.
# Uses curl for downloading files in parallel.

# Default values
VERSION="2025-11"
WIKI="enwiki"
BASE_URL="https://dumps.wikimedia.org/other/mediawiki_history"
OUTPUT_DIR="dataset/wikimedia_dumps"
START_DATE="2001-01"
END_DATE=""
DRY_RUN=0

# Function to show usage
show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -v VERSION     Dump version (default: 2025-10)"
    echo "  -w WIKI        Wiki name (default: enwiki)"
    echo "  -d DIR         Download directory (default: dataset/wikimedia_dumps)"
    echo "  -s START_DATE  Start date YYYY-MM (default: 2001-01)"
    echo "  -e END_DATE    End date YYYY-MM (default: up to VERSION)"
    echo "  -n             Dry run (generate URLs only, do not download)"
    echo "  -h             Show this help"
    echo ""
    echo "Examples:"
    echo "  $0 -s 2024-01 -e 2024-12"
    echo "  $0 -w dewiki -d ./my_data"
}

# Parse arguments
while getopts ":v:w:d:s:e:nh" opt; do
  case $opt in
    v) VERSION="$OPTARG" ;;
    w) WIKI="$OPTARG" ;;
    d) OUTPUT_DIR="$OPTARG" ;;
    s) START_DATE="$OPTARG" ;;
    e) END_DATE="$OPTARG" ;;
    n) DRY_RUN=1 ;;
    h) show_help; exit 0 ;;
    \?) echo "Invalid option: -$OPTARG" >&2; exit 1 ;;
  esac
done

# Basic validation
validate_date() {
    if echo "$1" | grep -q '^[0-9]\{4\}-[0-9]\{2\}$'; then
        return 0
    else
        echo "Error: Date $1 must be YYYY-MM"
        exit 1
    fi
}

validate_date "$VERSION"
validate_date "$START_DATE"
if [ -n "$END_DATE" ]; then
    validate_date "$END_DATE"
fi

# Set effective end date
if [ -z "$END_DATE" ]; then
    REAL_END="$VERSION"
else
    if [ "$END_DATE" \> "$VERSION" ]; then
        echo "Warning: End date $END_DATE exceeds version $VERSION. Clamping."
        REAL_END="$VERSION"
    else
        REAL_END="$END_DATE"
    fi
fi

echo "Configuration:"
echo "  Version:   $VERSION"
echo "  Wiki:      $WIKI"
echo "  Range:     $START_DATE to $REAL_END"
echo "  Directory: $OUTPUT_DIR"
if [ "$DRY_RUN" -eq 1 ]; then
    echo "  Mode:      Dry Run (No download)"
fi
echo ""

# Create directory if it doesn't exist
if [ ! -d "$OUTPUT_DIR" ]; then
    echo "Creating directory: $OUTPUT_DIR"
    mkdir -p "$OUTPUT_DIR"
fi

# Create a temporary file for URLs
URL_FILE="${OUTPUT_DIR}/urls_${VERSION}_${WIKI}.txt"
> "$URL_FILE"

# Extract components for loop
S_YEAR=${START_DATE%%-*}
S_MONTH=${START_DATE##*-}
E_YEAR=${REAL_END%%-*}
E_MONTH=${REAL_END##*-}

# Normalize months (remove leading zero for math)
S_MONTH=$(echo "$S_MONTH" | sed 's/^0*//')
E_MONTH=$(echo "$E_MONTH" | sed 's/^0*//')

count=0

# Loop through years
year=$S_YEAR
while [ "$year" -le "$E_YEAR" ]; do

    if [ "$year" -eq "$S_YEAR" ]; then month_start=$S_MONTH; else month_start=1; fi
    if [ "$year" -eq "$E_YEAR" ]; then month_end=$E_MONTH; else month_end=12; fi

    month=$month_start
    while [ "$month" -le "$month_end" ]; do

        if [ "$month" -lt 10 ]; then m_str="0$month"; else m_str="$month"; fi

        date_str="${year}-${m_str}"
        url="${BASE_URL}/${VERSION}/${WIKI}/${VERSION}.${WIKI}.${date_str}.tsv.bz2"

        echo "$url" >> "$URL_FILE"
        count=$((count + 1))

        month=$((month + 1))
    done
    year=$((year + 1))
done

echo "Generated $count URLs."

# Download Phase
if [ "$DRY_RUN" -eq 0 ]; then
    echo "Starting download of $count files..."

    # Check for aria2c (Best experience)
    if command -v aria2c >/dev/null 2>&1; then
        echo "Using aria2c for optimized parallel downloading..."
        echo ""

            aria2c \
                --input-file="$URL_FILE" \
                --dir="$OUTPUT_DIR" \
                --max-concurrent-downloads=1 \
                --max-connection-per-server=2 \
                --min-split-size=20M \
                --split=2 \
                --continue=true \
                --max-tries=0 \
                --retry-wait=10 \
                --user-agent="WikimediaDumpDownloader/1.0 (contact: marco.galeri@studio.unibo.it)"

    # Fallback to curl (Sequential but clean progress bar)
    elif command -v curl >/dev/null 2>&1; then
        echo "Note: 'aria2c' not found. Using 'curl' sequentially to show progress bar."
        echo "Install aria2 for faster parallel downloads."
        echo ""

        # Sequential loop to allow a clean progress bar per file
        current=1
        while IFS= read -r url; do
            filename=$(basename "$url")
            echo "[$current/$count] Downloading $filename..."

            # -# shows the progress bar
            curl -# -C - -o "$OUTPUT_DIR/$filename" "$url"

            current=$((current + 1))
        done < "$URL_FILE"

    else
        echo "Error: Neither aria2c nor curl is installed."
        exit 1
    fi

    echo "All downloads complete."
else
    echo "Dry run complete. URL list saved to: $URL_FILE"
fi
