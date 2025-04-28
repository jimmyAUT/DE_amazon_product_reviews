from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, year, month, dayofmonth, coalesce
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType


spark = SparkSession.builder.appName("CleanReviewData").getOrCreate()


review_schema = StructType([
    StructField("timestamp", LongType(), True),
    StructField("rating", DoubleType(), True),
    StructField("helpful_votes", LongType(), True),
    StructField("title", StringType(), True),
    StructField("text", StringType(), True),
    StructField("asin", StringType(), True),
    StructField("verified_purchase", BooleanType(), True),
    StructField("parent_asin", StringType(), True),
    StructField("user_id", StringType(), True),
])



df = spark.read \
    .schema(review_schema) \
    .option("mergeSchema", "true") \
    .json("gs://de-amazon-product-review-bucket/raw/reviews/*.jsonl.gz")

# Clearify what column name used in different raw data set
columns = df.columns
if "sort_timestamp" in columns:
    ts_col = col("sort_timestamp")
elif "timestamp" in columns:
    ts_col = col("timestamp")
else:
    raise Exception("Neither 'sort_timestamp' nor 'timestamp' column exists in the input data")


# Unify 'timestamp' column name
# total 571M rows, repartition to 200
df_clean = df.withColumn("timestamp_ms", ts_col) \
    .withColumn("timestamp", from_unixtime(col("timestamp_ms") / 1000).cast("timestamp")) \
    .select(
    col("timestamp_ms"),
    col("rating"),
    col("helpful_votes"),
    col("title"),
    col("text"),
    col("asin"),
    col("verified_purchase"),
    col("parent_asin"),
    col("user_id"),
    year(col("timestamp")).alias("year"),
    month(col("timestamp")).alias("month"),
    dayofmonth(col("timestamp")).alias("day"),
    col("timestamp")
).repartition(200)

# store as Parquet files to gs://bucket/staging/review/
df_clean.write.mode("overwrite").parquet("gs://de-amazon-product-review-bucket/staging/review/")