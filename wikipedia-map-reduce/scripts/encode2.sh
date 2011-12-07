# This script processes a dump file by file.
#

dest=$1
num_procs=15
shift

echo $@ | tr ' ' ' \n' | xargs -n 1 -P $num_procs ./scripts/reencode_one.sh $dest
