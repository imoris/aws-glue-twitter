import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, year, month, dayofmonth
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "twittersample", table_name = "raw_data", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "twittersample", table_name = "raw_data", transformation_ctx = "datasource0")

spark_df = datasource0.toDF()
new_df1 = spark_df.withColumn("year", year((col("timestamp_ms") / 1000).cast("timestamp")))
new_df2 = new_df1.withColumn("month", month((col("timestamp_ms") / 1000).cast("timestamp")))
new_df3 = new_df2.withColumn("day", dayofmonth((col("timestamp_ms") / 1000).cast("timestamp")))

new_datasource0 = DynamicFrame.fromDF(new_df3, glueContext, "new_datasource0")

## @type: ApplyMapping
## @args: [mapping = [("created_at", "string", "created_at", "string"), ("id", "long", "id", "long"), ("id_str", "string", "id_str", "string"), ("text", "string", "text", "string"), ("display_text_range", "array", "display_text_range", "array"), ("source", "string", "source", "string"), ("truncated", "boolean", "truncated", "boolean"), ("in_reply_to_status_id", "long", "in_reply_to_status_id", "long"), ("in_reply_to_status_id_str", "string", "in_reply_to_status_id_str", "string"), ("in_reply_to_user_id", "long", "in_reply_to_user_id", "long"), ("in_reply_to_user_id_str", "string", "in_reply_to_user_id_str", "string"), ("in_reply_to_screen_name", "string", "in_reply_to_screen_name", "string"), ("user", "struct", "user", "struct"), ("geo", "struct", "geo", "struct"), ("coordinates", "struct", "coordinates", "struct"), ("place", "struct", "place", "struct"), ("contributors", "string", "contributors", "string"), ("is_quote_status", "boolean", "is_quote_status", "boolean"), ("quote_count", "int", "quote_count", "int"), ("reply_count", "int", "reply_count", "int"), ("retweet_count", "int", "retweet_count", "int"), ("favorite_count", "int", "favorite_count", "int"), ("entities", "struct", "entities", "struct"), ("extended_entities", "struct", "extended_entities", "struct"), ("favorited", "boolean", "favorited", "boolean"), ("retweeted", "boolean", "retweeted", "boolean"), ("possibly_sensitive", "boolean", "possibly_sensitive", "boolean"), ("filter_level", "string", "filter_level", "string"), ("lang", "string", "lang", "string"), ("timestamp_ms", "string", "timestamp_ms", "string"), ("retweeted_status", "struct", "retweeted_status", "struct"), ("quoted_status_id", "long", "quoted_status_id", "long"), ("quoted_status_id_str", "string", "quoted_status_id_str", "string"), ("quoted_status", "struct", "quoted_status", "struct"), ("extended_tweet", "struct", "extended_tweet", "struct")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
#applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("created_at", "string", "created_at", "string"), ("id", "long", "id", "long"), ("id_str", "string", "id_str", "string"), ("text", "string", "text", "string"), ("display_text_range", "array", "display_text_range", "array"), ("source", "string", "source", "string"), ("truncated", "boolean", "truncated", "boolean"), ("in_reply_to_status_id", "long", "in_reply_to_status_id", "long"), ("in_reply_to_status_id_str", "string", "in_reply_to_status_id_str", "string"), ("in_reply_to_user_id", "long", "in_reply_to_user_id", "long"), ("in_reply_to_user_id_str", "string", "in_reply_to_user_id_str", "string"), ("in_reply_to_screen_name", "string", "in_reply_to_screen_name", "string"), ("user", "struct", "user", "struct"), ("geo", "struct", "geo", "struct"), ("coordinates", "struct", "coordinates", "struct"), ("place", "struct", "place", "struct"), ("contributors", "string", "contributors", "string"), ("is_quote_status", "boolean", "is_quote_status", "boolean"), ("quote_count", "int", "quote_count", "int"), ("reply_count", "int", "reply_count", "int"), ("retweet_count", "int", "retweet_count", "int"), ("favorite_count", "int", "favorite_count", "int"), ("entities", "struct", "entities", "struct"), ("extended_entities", "struct", "extended_entities", "struct"), ("favorited", "boolean", "favorited", "boolean"), ("retweeted", "boolean", "retweeted", "boolean"), ("possibly_sensitive", "boolean", "possibly_sensitive", "boolean"), ("filter_level", "string", "filter_level", "string"), ("lang", "string", "lang", "string"), ("timestamp_ms", "string", "timestamp_ms", "string"), ("retweeted_status", "struct", "retweeted_status", "struct"), ("quoted_status_id", "long", "quoted_status_id", "long"), ("quoted_status_id_str", "string", "quoted_status_id_str", "string"), ("quoted_status", "struct", "quoted_status", "struct"), ("extended_tweet", "struct", "extended_tweet", "struct")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
#resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
#dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://twsch-firehose/tmp/parquet-partitioned"}, format = "parquet", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_options(frame = new_datasource0, connection_type = "s3", connection_options = {"path": "s3://twsch-firehose/tmp/parquet-partitioned", "partitionKeys": ["year","month","day"]}, format = "parquet", transformation_ctx = "datasink4")
job.commit()