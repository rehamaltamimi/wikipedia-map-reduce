#!/bin/csh

# Add tenure information to extracted datafiles.

# Specify a starting index (0) and an ending index (the xxxxx in the last part-r-xxxxx you want to process)
# Specify a directory where the input files are.
# Specify a directory where the output files will go.
# Specify a directory where the python addtenure script lives.

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

echo Adding tenure to files $nametemplate from $begin to $end with input from $inputdir, output to $outputdir, and addtenure script from $tooldir.

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
	
	echo Adding tenure to file $filename.
	
	cat $inputdir/$filename | python $tooldir/addTenure.py > $outputdir/$filename
	
	echo Finished adding tenure to file $filename.

	@ index = $index + 1

end

echo Finished adding tenure to files $nametemplate from $begin to $end with input from $inputdir, output to $outputdir, and addtenure script from $tooldir.
