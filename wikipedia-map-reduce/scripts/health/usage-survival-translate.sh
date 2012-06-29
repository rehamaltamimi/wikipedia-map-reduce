#!/bin/csh

set begin = $1
set end = $2
set tooldir = $3

set nametemplate = "part-r-"

set index = $begin
set numchars = 0
set filename = nametemplate
set fileindex = "0"

echo `date` Translating files $nametemplate from $begin to $end using tools from $tooldir.

mkdir ./out
mkdir ./cleaned
mkdir ./sorted
mkdir ./sessions

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
	
	echo `date` Translating file $filename.
	
	echo `date` Sanitizing usernames, removing anons and pre-2003 data.
	cat ./$filename | python $tooldir/cleanNames.py > ./cleaned/$filename

	echo `date` Sorting user, timestamp data.
	sort -o ./sorted/$filename ./cleaned/$filename

	echo `date` Building session data.
	java -jar $tooldir/SessionBreaker.jar ./sorted/$filename ./sessions/$filename

	echo `date` Translating usage and survival data.
	cat ./sessions/$filename | python $tooldir/obtainInfo.py > ./out/$filename
	
	echo `date` Finished translating file $filename.

	@ index = $index + 1

end

echo `date` Finished translating.
