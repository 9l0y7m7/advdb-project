#!/bin/bash

rm -rf output 
mkdir output
count=1
for filename in `ls test/* | sort -V`; do
    file=$(basename "$filename")
    (python main.py -f $filename) &> output/run_$file
    echo res/res$count output/run_$file
    if diff res/res$count output/run_$file; then
        echo "run_$filename pass"
    fi
    (( count++ ))
done