current=""
if [ $1 == "current" ]; then
    current="current"
    shift
fi

dest_dir=$1
log_dir=$1/logs
src=$2
file=`basename $src`
res=$dest_dir/$file.hadoop
7za e -so $src 2>$log_dir/$file.7z.log |  python2.6 ./scripts/encode.py $current $res 2>$log_dir/$file.encode.log
