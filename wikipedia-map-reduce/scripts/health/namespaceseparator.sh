#!/bin/csh

# Separate namespace and tenure data by namespace, and by namespace and year

# Specify a starting index (0) and an ending index (the xxxxx in the last part-r-xxxxx you want to process)
# Specify a directory where the input files are.
# Specify a directory where the output files will go.
# There will be an output file for each namespace, and an output file for each namespace by year.
# These output files are appended by the script when running on each consecutive input datafile
# Specify a directory where the python namespace separator script lives.

set begin = $1
set end = $2
set inputdir = $3 
set outputdir = $4
set tooldir = $5

set nametemplate = "part-r-"

set index = $begin
set numchars = 0
set filename = nametemplate
set fileindex = "0"

echo `date` Separating by namespace and year from files $nametemplate from $begin to $end with input from $inputdir, output to $outputdir, and separator script from $tooldir.

mkdir $outputdir
mkdir $outputdir/namespaces
mkdir $outputdir/namespaces/years

cp $tooldir/namespaceSeparator.py $outputdir 	# The script must run from the directory where output should go, as it puts it in a folder inside there

cd $outputdir 					# Run the script from this folder

while ($index <= $end)

	# setup the file name to use the five-digit emr indices

	@ numchars = `echo $index | wc -m`
	if ($numchars == 2) then
		set fileindex = "0000"$index
	else if ($numchars == 3) then
		set fileindex = "000"$index
	else if ($numchars == 4) then
		set fileindex = "00"$index
	else if ($numchars == 5) then
		set fileindex = "0"$index
	else 
		set fileindex = $index
	endif

	set filename = ${nametemplate}${fileindex}
	
	echo `date` Separating by namespace to file $filename.
	
	python $outputdir/namespaceSeparator.py $inputdir/$filename
	
	echo `date` Finished separating by namespace from file $filename.

	@ index = $index + 1

end

rm $outputdir/namespaceSeparator.py		# Get rid of the script when it's done to clean up

echo `date` Finished separating by namespace and year from files $nametemplate from $begin to $end with input from $inputdir, output to $outputdir, and separator script from $tooldir.
