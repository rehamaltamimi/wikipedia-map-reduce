dest_dir=$1
src=$2
file=`basename $src`
res=$dest_dir/$file.hadoop
log_py=$dest_dir/$file.hadoop.log
log_7z=$dest_dir/$file.7z.log
7za e -so $src 2>$log_7z | python2.6 ./scripts/reencode.py $res 2>$log_py
