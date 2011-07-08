filedest=$1
shift
rm $filedest/wikipedia.txt.*
rm $filedest/encode.log
(for i in $@; do
	7za e -so $i
done ) | python ./split.py 100 $filedest/wikipedia.txt. 8000 >&$filedest/encode.log

#python ./split.py 100 $1/wikipedia.txt. 8000 >&$1/encode.log 

#7za e -so $1 | python ./split.py 100 $2/wikipedia.txt. 8000 >&$2/encode.log
