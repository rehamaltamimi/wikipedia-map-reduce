#!/bin/csh

# Separates edit tenure data by year, using a new process for 

# Specify a filename to work on (must be a total path unless running this from the input directory)
# Specify an output directory
# The tool script must be in the output directory (set up auto if you use the parallel script)

set filename = $1
set outputdir = $2

cd $outputdir

echo `date` Separating by year from file $filename.

python $outputdir/startAndOffsetEdits.py $filename

echo `date` Finished separating by year from file $filename.
