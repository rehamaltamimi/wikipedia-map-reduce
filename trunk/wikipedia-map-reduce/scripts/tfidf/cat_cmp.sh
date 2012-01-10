jar_dest=jobjars/$RANDOM/wikipediamapreduce.jar
echo "placing jar in $jar_dest"

s3cmd put ./dist/wikipedia-map-reduce.jar s3://macademia/$jar_dest || exit 1

elastic-mapreduce --create --name 'CategoryComparer' \
    --instance-group master --instance-type cc2.8xlarge \
    --instance-count 1 \
    --instance-group core --instance-type cc2.8xlarge \
    --instance-count 3 \
    --instance-group task --instance-type cc2.8xlarge \
    --instance-count 16 --bid-price 1.00  \
    --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop \
    --args "-s,mapred.map.tasks=500,-s,mapred.reduce.tasks=750,-s,mapred.tasktracker.map.tasks.maximum=20,-s,mapred.task.timeout=1200000,-s,mapred.output.compress=true" \
    --log-uri s3://macademia/nbrvz/logs \
    --enable-debugging \
    --jar s3n://macademia/$jar_dest \
    --main-class wmr.categories.HadoopCategoryComparer \
    --args "s3n://macalester/wikipedia/categories,s3n://macademia/nbrvz/categories/sims/,s3n://macademia/nbrvz/categories/all_cats.txt.gz"
