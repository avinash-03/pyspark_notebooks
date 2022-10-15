LOG_DIR="s3a://avinashhivedata/spark/"
AWS_ACCESS_KEY_ID="asdfhjodfvbnmwetyu"
AWS_SECRET_ACCESS_KEY="sdfghjitedbwdfghergh"

docker create \
	-e SPARK_HISTORY_OPTS="$SPARK_HISTORY_OPTS \
	-Dspark.history.fs.logDirectory=$LOG_DIR \
	-Dspark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID \
	-Dspark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY" \
	-p 18080:18080 \
	--name sparkui \
	glue/sparkui:latest \
	"/opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer"
