set -x

bucket=s3://macalester                          # s3 bucket
cites=$bucket/cites                            # everything for cites goes under here
input=$bucket/wikipedia/en-2012-05-02/          # the processed full text input file
output=$cites/out/final/                         # output directory. CAREFUL - WILL BE OVERWRITTEN!
logs=$cites/logs
class=wmr.citations.FinalCitationCounter

jar_dest=$cites/jobjars/$RANDOM/wikipediamapreduce.jar
echo "placing jar in $jar_dest"


s3cmd put ./dist/wikipedia-map-reduce.jar $jar_dest || exit 1

elastic-mapreduce --create --name 'FinalCitationCounter' \
    --instance-group master --instance-type cc2.8xlarge \
    --instance-count 1 \
    --instance-group core --instance-type cc2.8xlarge \
    --instance-count 4 \
    --instance-group task --instance-type cc2.8xlarge \
    --instance-count 50 --bid-price 2.00  \
    --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop \
    --args "-s,mapred.task.timeout=2000000" \
    --log-uri $logs \
    --enable-debugging \
    --jar $jar_dest \
    --main-class $class \
    --args "$input,$output" \
    --availability-zone us-east-1e 
