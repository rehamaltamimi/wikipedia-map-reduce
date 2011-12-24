#!/bin/bash

if [ -z "$1" ] || [ -z "$2" ]; then
    echo "usage: $0 s3://dest/dir file1 file2 ...."
fi

dest=$1
shift
echo "dest is $dest"

echo $@ | tr ' ' '\n' | parallel -P 5 --group s3cmd put {} $dest 
