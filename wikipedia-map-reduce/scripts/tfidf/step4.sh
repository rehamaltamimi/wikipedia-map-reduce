jar_dest=jobjars/$RANDOM/wikipediamapreduce.jar
echo "placing jar in $jar_dest"

s3cmd put ./dist/wikipedia-map-reduce.jar s3://macademia/$jar_dest || exit 1

elastic-mapreduce --create --name 'Step4DocSimScorer' \
    --instance-group master --instance-type cc2.8xlarge \
    --instance-count 1 \
    --instance-group core --instance-type cc2.8xlarge \
    --instance-count 5 \
    --instance-group task --instance-type cc2.8xlarge \
    --instance-count 14 --bid-price 1.00  \
    --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop \
    --args "-s,mapred.compress.map.output=false,-s,mapred.map.tasks=500,-s,mapred.reduce.tasks=750,-s,mapred.tasktracker.reduce.tasks.maximum=16" \
    --log-uri s3://macademia/nbrvz/logs \
    --enable-debugging \
    --jar s3n://macademia/$jar_dest \
    --main-class wmr.tfidf.Step4DocSimScorer \
    --args "s3n://macademia/nbrvz/tf-idf-words/step3/,s3n://macademia/nbrvz/tf-idf-words/step4/,2500"
