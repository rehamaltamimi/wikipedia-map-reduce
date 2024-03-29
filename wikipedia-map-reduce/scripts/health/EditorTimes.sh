bucket=s3://macalester                          # s3 bucket
health=$bucket/health                           # everything for maxima goes under here
input=$bucket/wikipedia/en-2011-11-15/          # the processed full text input file
output=$health/out/editortimes/                 # output directory. CAREFUL - WILL BE OVERWRITTEN!
logs=$health/logs
class=wmr.health.EditorTimes

jar_dest=$health/jobjars/$RANDOM/wikipediamapreduce.jar
echo "placing jar in $jar_dest"


s3cmd put ./dist/wikipedia-map-reduce.jar $jar_dest || exit 1

elastic-mapreduce --create --name 'EditorTimes' \
    --instance-group master --instance-type cc2.8xlarge \
    --instance-count 1 \
    --instance-group core --instance-type cc2.8xlarge \
    --instance-count 1 \
    --instance-group task --instance-type cc2.8xlarge \
    --instance-count 6 --bid-price 2.00  \
    --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop \
    --args "-s,mapred.task.timeout=2000000" \
    --log-uri $logs \
    --enable-debugging \
    --jar $jar_dest \
    --main-class $class \
    --args "$input,$output" \
    --availability-zone us-east-1e 
