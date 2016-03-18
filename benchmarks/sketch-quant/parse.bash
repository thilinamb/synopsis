#!/usr/bin/env bash

# Generates test configurations from raw quantization output

IFS=$'\n'
for line in $(cat ticks.txt); do
    IFS=$' \t'
    items=(${line})
    echo "        quantizers.put(\"${items[0]}\", new Quantizer("
    for (( i = 1; i < ${#items[@]} - 1; i++ )); do
        echo "                    new Feature(${items[$i]}f),"
    done
    echo "                    new Feature(${items[${#items[@]} - 1]}f)"
    echo "        ));"
done
