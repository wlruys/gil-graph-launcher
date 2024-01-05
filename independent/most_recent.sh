#!/bin/bash

# Find the most recent file in the current directory
most_recent_file=$(ls -t | head -n 1)

# Check if a file was found
if [ -n "$most_recent_file" ]; then
    cat "$most_recent_file"
else
    echo "No files found in the current directory."
fi

