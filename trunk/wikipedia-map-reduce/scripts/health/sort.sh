#!/bin/csh

set begin = $1
set end = $2

set nametemplate = "part-r-"

set index = $begin
set numchars = 0
set filename = nametemplate
set fileindex = "0"

echo Sorting files $nametemplate from $begin to $end.

mkdir ./sorted

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
	
	echo Sorting file $filename.

	sort -o ./sorted/$filename ./$filename
	
	echo Finished sorting file $filename.

	@ index = $index + 1

end
