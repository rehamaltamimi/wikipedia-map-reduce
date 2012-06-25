#!/bin/csh

set begin = $1
set end = $2
set tooldir = $3

set nametemplate = "part-r-"

set index = $begin
set numchars = 0
set filename = nametemplate
set fileindex = "0"

echo Translating files $nametemplate from $begin to $end using tools from $tooldir.

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
	
	echo Translating file $filename.
	
	echo Sanitizing usernames, removing anons and pre-2003 data.
	cat ./$filename | python $tooldir/cleanNames.py > ./cleaned/$filename

	echo Sorting user, timestamp data.
	sort -o ./sorted/$filename ./cleaned/$filename

	echo Building session data.
	java -jar $tooldir/SessionBreaker.jar ./sorted/$filename ./sessions/$filename

	echo Translating usage and survival data.
	cat ./sessions/$filename | python $tooldir/obtainInfo.py > ./out/$filename
	
	echo Finished translating file $filename.

	@ index = $index + 1

end

echo Finished translating.
