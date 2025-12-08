#!/bin/bash
# Download LDBC SNB Interactive v1 data from SURF repository
# Usage: ./download_data.sh [sf0.1|sf1|sf3|sf10]

set -e

SCALE_FACTOR=${1:-sf0.1}
DATA_DIR="$(dirname "$0")/../data"

# Map scale factor to download URL
case $SCALE_FACTOR in
    sf0.1)
        URL="https://repository.surfsara.nl/datasets/cwi/ldbc-snb-interactive-v1-datagen-v100/files/social_network-sf0.1-CsvBasic-LongDateFormatter.tar.zst"
        ;;
    sf0.3)
        URL="https://repository.surfsara.nl/datasets/cwi/ldbc-snb-interactive-v1-datagen-v100/files/social_network-sf0.3-CsvBasic-LongDateFormatter.tar.zst"
        ;;
    sf1)
        URL="https://repository.surfsara.nl/datasets/cwi/ldbc-snb-interactive-v1-datagen-v100/files/social_network-sf1-CsvBasic-LongDateFormatter.tar.zst"
        ;;
    sf3)
        URL="https://repository.surfsara.nl/datasets/cwi/ldbc-snb-interactive-v1-datagen-v100/files/social_network-sf3-CsvBasic-LongDateFormatter.tar.zst"
        ;;
    sf10)
        URL="https://repository.surfsara.nl/datasets/cwi/ldbc-snb-interactive-v1-datagen-v100/files/social_network-sf10-CsvBasic-LongDateFormatter.tar.zst"
        ;;
    *)
        echo "Unknown scale factor: $SCALE_FACTOR"
        echo "Usage: $0 [sf0.1|sf0.3|sf1|sf3|sf10]"
        exit 1
        ;;
esac

# Create data directory
mkdir -p "$DATA_DIR"
cd "$DATA_DIR"

FILENAME="social_network-${SCALE_FACTOR}-CsvBasic-LongDateFormatter.tar.zst"

echo "=============================================="
echo "LDBC SNB Data Download"
echo "=============================================="
echo "Scale Factor: $SCALE_FACTOR"
echo "URL: $URL"
echo "Target: $DATA_DIR"
echo "=============================================="

# Download if not exists
if [ ! -f "$FILENAME" ]; then
    echo "Downloading $FILENAME..."
    wget -c "$URL" -O "$FILENAME"
else
    echo "File already exists: $FILENAME"
fi

# Extract
echo "Extracting..."
if command -v zstd &> /dev/null; then
    zstd -d "$FILENAME" --stdout | tar xf -
else
    # Fallback to tar with zstd plugin
    tar --use-compress-program=unzstd -xf "$FILENAME"
fi

# Rename extracted directory for consistency
if [ -d "social_network-${SCALE_FACTOR}-CsvBasic-LongDateFormatter" ]; then
    mv "social_network-${SCALE_FACTOR}-CsvBasic-LongDateFormatter" "$SCALE_FACTOR"
fi

echo "=============================================="
echo "Download complete!"
echo "Data extracted to: $DATA_DIR/$SCALE_FACTOR"
echo ""
echo "Directory structure:"
ls -la "$SCALE_FACTOR"/ 2>/dev/null || echo "(directory not found)"
echo "=============================================="
