#!/bin/csh

set begin = $1
set end = $2
set tooldir = $3

set nametemplate = "part-r-"

set index = $begin

echo Translating files $nametemplate from $begin to $end using tools from $tooldir.

mkdir ./usage
mkdir ./survival
mkdir ./tmp
mkdir ./tmp/sanitized
mkdir ./tmp/sorted
mkdir ./tmp/sessions

while ($index <= $end)
	
	echo Sanitizing usernames in user, timestamp data.
	# sanitize with python

	echo Removing anonymous entries from user, timestamp data.
	# remove anon addresses with python

	echo Sorting user, timestamp data.
	sort -o ./tmp/sorted/$nametemplate$index ./$nametemplate$index

	echo Building session data.
	java -jar $tooldir/SessionBreaker.jar ./tmp/sanitized/$nametemplate$index ./tmp/sessions/$nametemplate$index

	echo Translating usage data.
	# translate usage data with python

	echo Translating survival data.
	# translate survival data with python

	$index = $index + 1

end

echo Finished translating.

echo Cleaning up...
rm -r ./tmp
echo Done!
