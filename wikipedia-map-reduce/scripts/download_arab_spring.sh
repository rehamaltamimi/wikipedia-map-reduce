dir=../dat/arab_spring

export PYTHONPATH=.:~/Downloads/pywikipedia/

while read page; do
    title=`echo $page | grep -v '^#' | sed 's/.*\///'`
    if [ -z "$title" ]; then
        continue
    fi
    echo "downloading page $title"
    python2.6 ./encode_api.py "$dir/$title.txt" $title
done < $dir/pages.txt
