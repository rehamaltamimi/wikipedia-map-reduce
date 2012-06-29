#!/bin/csh

#################################################################################
# This works differently now, don't use this unless you know what you're doing. #
#################################################################################

# Build statistics about namespace data that was processed by namespaceSeparator.py
# Takes an input directory where the namespaces/years output directory from the separator script is. (this dir is called 'separated' the way we do it).
# Takes an output directory where the built statistics file will be written out to.
# Specify a directory where the namespace stats script is.

set inputdir = $1
set outputdir = $2
set tooldir = $3

mkdir $outputdir

cp $tooldir/statsNamespace.py $inputdir	# Move the script here for processing temporarily

cd $inputdir 				# The script must be run in the directory where the 'namespaces' output dir from the separator script is.

echo `date` Building namespace statistics from data in $inputdir and writing output to $outputdir with statistics script from $tooldir.

python $inputdir/statsNamespace.py > $outputdir/namespacestats

rm $inputdir/statsNamespace.py		# Remove the script when done.

echo `date` Finished building namespace statistics from data in $inputdir and output to $outputdir with statistics script from $tooldir.


