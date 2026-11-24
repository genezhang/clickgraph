#!/bin/bash
# Generate LDBC SNB data locally with the LDBC datagen Docker image.
#
# Usage: ./download_data.sh [sf0.003|sf0.1|sf1|sf3|sf10] [--generate]
#
# The CSV tree lands in data/<scale-factor>/graphs/csv/interactive/
# composite-projected-fk/ and is loaded by scripts/load_data.py.

set -e

SCALE_FACTOR=${1:-sf0.003}
METHOD=${2:---generate}
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$SCRIPT_DIR/../data"
OUTPUT_DIR="$DATA_DIR/$SCALE_FACTOR"

if [ "$METHOD" != "--generate" ]; then
    echo "Unknown method: $METHOD"
    echo "Usage: $0 <scale-factor> [--generate]"
    exit 1
fi

# Extract numeric scale factor
SF_NUM=${SCALE_FACTOR#sf}

echo "=============================================="
echo "LDBC SNB Data Generator"
echo "=============================================="
echo "Scale Factor: $SCALE_FACTOR (SF=$SF_NUM)"
echo "Output: $OUTPUT_DIR"
echo "=============================================="

if [ -f "$OUTPUT_DIR/.generated" ]; then
    echo "Data already generated; delete $OUTPUT_DIR to regenerate."
    exit 0
fi

if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker is required but not installed."
    echo "See https://docs.docker.com/get-docker/"
    exit 1
fi

mkdir -p "$OUTPUT_DIR"

echo ""
echo "Running LDBC datagen. sf1 takes roughly 30 minutes."
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

touch "$OUTPUT_DIR/.generated"

echo ""
echo "=============================================="
echo "Generation complete: $OUTPUT_DIR"
echo "Next: python3 scripts/load_data.py --scale-factor $SCALE_FACTOR"
echo "=============================================="
