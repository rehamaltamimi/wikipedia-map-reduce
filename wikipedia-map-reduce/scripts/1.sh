set -x

bucket=s3://macalester                          # s3 bucket
cites=$bucket/cites                             # everything for cites goes under here
input=$bucket/wikipedia/ar-2012-05-03/          # the processed full text input file
output=$bucket/wikipedia/ar-2012-05-03-sorted/   # output directory. CAREFUL - WILL BE OVERWRITTEN! 
logs=$cites/logs
class=wmr.tstampsorter.TstampSorterMain

jar_dest=$cites/jobjars/$RANDOM/wikipediamapreduce.jar
echo "placing jar in $jar_dest"


s3cmd put ./dist/wikipedia-map-reduce.jar $jar_dest || exit 1

elastic-mapreduce --create --name 'TstampSorter' \
    --instance-group master --instance-type cc2.8xlarge \
    --instance-count 1 \
    --instance-group core --instance-type cc2.8xlarge \
    --instance-count 3 \
    --instance-group task --instance-type cc2.8xlarge \
    --instance-count 20 --bid-price 3.00  \
    --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop \
    --args "-s,mapred.task.timeout=2000000,-s,mapred.reduce.tasks=2000,-s,mapred.tasktracker.reduce.tasks.maximum=16,-s,mapred.tasktracker.map.tasks.maximum=16" \
    --log-uri $logs \
    --enable-debugging \
    --jar $jar_dest \
    --main-class $class \
    --args "$input,$output" \
    --availability-zone us-east-1e 
