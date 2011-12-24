# This script processes a dump file by file.
# The parallel command can be downloaded at http://www.gnu.org/software/parallel/
#

if [ $1 -eq "current" ]; then
    current="current"
    shift
fi

dest=$1
shift

mkdir -p $dest/logs

echo $@ | \
tr ' ' '\n' | \
parallel ./scripts/reencode_one.sh $current $dest {}
