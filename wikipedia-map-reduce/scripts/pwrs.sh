bucket=s3://macalester                          # s3 bucket
input=$bucket/wikipedia/en-2012-05-02-sorted/   # the processed full text input file
pwrs=$bucket/wikipedia/pwrs                     # pwr directory
output=$pwrs/out/                               # output directory. CAREFUL - WILL BE OVERWRITTEN!
logs=$pwrs/logs/
class=wmr.wmf.PersistentWordRevisions

jar_dest=$pwrs/jobjars/$RANDOM/wikipediamapreduce.jar
echo "placing jar in $jar_dest"


s3cmd put ./dist/wikipedia-map-reduce.jar $jar_dest || exit 1

elastic-mapreduce --create --name 'FeatureGenerator' \
    --instance-group master --instance-type cc2.8xlarge \
    --instance-count 1 \
    --instance-group core --instance-type cc2.8xlarge \
    --instance-count 3 \
    --instance-group task --instance-type cc2.8xlarge \
    --instance-count 30 --bid-price 5.00  \
    --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop \
    --args "-s,mapred.task.timeout=2000000" \
    --log-uri $logs \
    --enable-debugging \
    --jar $jar_dest \
    --main-class $class \
    --args "$input,$output" \
    --availability-zone us-east-1e 
