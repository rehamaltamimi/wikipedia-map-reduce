#!/bin/csh

# Clean extracted datafiles to remove anonymous contributors and data that is before 2003
# Specify a starting index (0) and an ending index (the xxxxx in the last part-r-xxxxx you want to clean)
# Specify a directory where the input files are.
# Specify a directory where the output files will go.
# Specify a directory where the python clean script lives.

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

echo `date` Cleaning files $nametemplate from $begin to $end with input from $inputdir, output to $outputdir, and clean script from $tooldir.

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
	
	echo `date` Cleaning file $filename.
	
	cat $inputdir/$filename | python $tooldir/cleanNames.py > $outputdir/$filename
	
	echo `date` Finished cleaning file $filename.

	@ index = $index + 1

end

echo `date` Finished cleaning files $nametemplate from $begin to $end with input from $inputdir, output to $outputdir, and clean script from $tooldir.
