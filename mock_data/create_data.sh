#!/bin/bash

# Define the base directory
base_dir="data"

# Create the directories
mkdir -p "$base_dir/output/events/person"
mkdir -p "$base_dir/output/discards/person"
mkdir -p "$base_dir/input/events/person"
mkdir -p "$base_dir/logs"

echo "Directories created successfully."

python3 mock_data/mock_data.py "$base_dir/input/events/person/person_data1.json"
python3 mock_data/mock_data.py "$base_dir/input/events/person/person_data2.json"

echo "Data created."