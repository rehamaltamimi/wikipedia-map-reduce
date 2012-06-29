#!/bin/csh

# Sort all the extracted datafiles using unix sort
# Specify a starting index (0) and an ending index (index of last part-r-xxxxx file where the index is xxxxx)\
# Specify a directory where the input files are.
# Specify a directory where the output files will go.
# Specify optional extra arguments to give to unix sort.

set begin = $1
set end = $2
set inputdir = $3
set outputdir = $4
set sortargs = $5

set nametemplate = "part-r-"

set index = $begin
set numchars = 0
set filename = nametemplate
set fileindex = "0"

echo `date` Sorting files $nametemplate from $begin to $end with input from $inputdir, output to $outputdir, and optional sort args $sortargs.

mkdir $outputdir

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
	
	echo `date` Sorting file $filename.

	sort -o $outputdir/$filename $inputdir/$filename
	
	echo `date` Finished sorting file $filename.

	@ index = $index + 1

end

echo `date` Finished sorting files $nametemplate from $begin to $end with input from $inputdir, output to $outputdir, and optional sort args $sortargs.
