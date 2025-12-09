#!/bin/bash
# Generate LDBC SNB data using Docker
# Usage: ./download_data.sh [sf0.003|sf0.1|sf1|sf3|sf10] [--generate|--download]
#
# Method 1 (recommended): Generate data locally using Docker
#   ./download_data.sh sf0.1 --generate
#
# Method 2: Try downloading from SURF (may not work due to access restrictions)
#   ./download_data.sh sf0.1 --download

set -e

SCALE_FACTOR=${1:-sf0.003}
METHOD=${2:---generate}
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$SCRIPT_DIR/../data"

# Extract numeric scale factor
SF_NUM=${SCALE_FACTOR#sf}

echo "=============================================="
echo "LDBC SNB Data Generator"
echo "=============================================="
echo "Scale Factor: $SCALE_FACTOR (SF=$SF_NUM)"
echo "Method: $METHOD"
echo "Target: $DATA_DIR"
echo "=============================================="

# Create data directory
mkdir -p "$DATA_DIR"
cd "$DATA_DIR"

if [ "$METHOD" = "--generate" ]; then
    echo ""
    echo "Generating data using Docker..."
    echo "This may take a few minutes for small scale factors."
    echo ""
    
    # Check if Docker is available
    if ! command -v docker &> /dev/null; then
        echo "ERROR: Docker is required but not installed."
        echo "Please install Docker first: https://docs.docker.com/get-docker/"
        exit 1
    fi
    
    # Create output directory
    OUTPUT_DIR="$DATA_DIR/$SCALE_FACTOR"
    mkdir -p "$OUTPUT_DIR"
    
    # Run the LDBC datagen using Docker
    # Using the standalone image with Interactive mode settings
    echo "Running LDBC Datagen Docker image..."
    echo "Output will be in: $OUTPUT_DIR"
    echo ""
    
    docker run --rm \
        --mount type=bind,source="$OUTPUT_DIR",target=/out \
        ldbc/datagen-standalone:0.5.1-2.12_spark3.2 \
        --parallelism 1 \
        --memory 4G \
        -- \
        --format csv \
        --scale-factor "$SF_NUM" \
        --mode interactive \
        --output-dir /out \
        --epoch-millis \
        --explode-edges
    
    echo ""
    echo "=============================================="
    echo "Generation complete!"
    echo "Data generated to: $OUTPUT_DIR"
    echo ""
    echo "Directory structure:"
    ls -la "$OUTPUT_DIR"/ 2>/dev/null || echo "(directory not found)"
    echo "=============================================="

elif [ "$METHOD" = "--download" ]; then
    echo ""
    echo "Attempting to download from SURF repository..."
    echo "Note: This may fail due to access restrictions."
    echo ""
    
    # Map scale factor to download URL (Interactive v1 format)
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
            echo "Unknown scale factor for download: $SCALE_FACTOR"
            echo "Available: sf0.1, sf0.3, sf1, sf3, sf10"
            echo ""
            echo "For other scale factors, use: $0 $SCALE_FACTOR --generate"
            exit 1
            ;;
    esac
    
    FILENAME="social_network-${SCALE_FACTOR}-CsvBasic-LongDateFormatter.tar.zst"
    
    # Download
    if [ ! -f "$FILENAME" ]; then
        echo "Downloading $FILENAME..."
        wget -c "$URL" -O "$FILENAME" --no-check-certificate || {
            echo ""
            echo "ERROR: Download failed. The SURF repository may have access restrictions."
            echo ""
            echo "Alternative: Generate data locally using Docker:"
            echo "  $0 $SCALE_FACTOR --generate"
            exit 1
        }
    else
        echo "File already exists: $FILENAME"
    fi
    
    # Extract
    echo "Extracting..."
    if command -v zstd &> /dev/null; then
        zstd -d "$FILENAME" --stdout | tar xf -
    else
        tar --use-compress-program=unzstd -xf "$FILENAME"
    fi
    
    # Rename extracted directory for consistency
    EXTRACTED_DIR="social_network-${SCALE_FACTOR}-CsvBasic-LongDateFormatter"
    if [ -d "$EXTRACTED_DIR" ]; then
        mv "$EXTRACTED_DIR" "$SCALE_FACTOR"
    fi
    
    echo "=============================================="
    echo "Download complete!"
    echo "Data extracted to: $DATA_DIR/$SCALE_FACTOR"
    echo ""
    echo "Directory structure:"
    ls -la "$SCALE_FACTOR"/ 2>/dev/null || echo "(directory not found)"
    echo "=============================================="

else
    echo "Unknown method: $METHOD"
    echo ""
    echo "Usage: $0 <scale-factor> [--generate|--download]"
    echo ""
    echo "Examples:"
    echo "  $0 sf0.003 --generate   # Generate tiny dataset (fastest, ~1 min)"
    echo "  $0 sf0.1 --generate     # Generate small dataset (~5 min)"
    echo "  $0 sf1 --generate       # Generate medium dataset (~30 min)"
    echo "  $0 sf0.1 --download     # Try downloading from SURF (may fail)"
    exit 1
fi
