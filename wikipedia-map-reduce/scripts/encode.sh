for i in ${2..$#}; do
	7za e -so $i | python ./split.py 100 $1/wikipedia.txt. 8000 >&$1/encode.log
done

#python ./split.py 100 $1/wikipedia.txt. 8000 >&$1/encode.log 

#7za e -so $1 | python ./split.py 100 $2/wikipedia.txt. 8000 >&$2/encode.log
