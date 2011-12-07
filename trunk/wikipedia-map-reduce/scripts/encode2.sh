# This script processes a dump file by file.
# The parallel command can be downloaded at http://www.gnu.org/software/parallel/
#

dest=$1
shift

mkdir -p $dest/logs

echo $@ | \
tr ' ' '\n' | \
parallel ./scripts/reencode_one.sh $dest {}
