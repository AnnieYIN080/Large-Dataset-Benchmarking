#!/bin/bash

# Immediately catch errors and ensure that the script exits safely and predictably
# To enhance the Robustness and reliability of the script
set -euo pipefail

# Define the local output directory name
OUTDIR="path/my_folder"

# Create the output directory if it does not exist (-p flag prevents errors if it already exists)
mkdir -p "$OUTDIR"

# Use 4 digits for the numeric suffix (e.g., chunk_0000).
# The prefix for the output files (e.g., path/my_folder/chunk_0000, path/my_folder/chunk_0001, etc.).
aws s3 cp "s3://path.csv" - | split -l 10000000 -d -a 4 - "$OUTDIR/chunk_"
