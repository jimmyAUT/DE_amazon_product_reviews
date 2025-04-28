from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, ArrayType, DoubleType


spark = SparkSession.builder.appName("CleanAmazonItem").getOrCreate()


schema = StructType()
schema = schema.add("main_category", StringType()) \
               .add("parent_asin", StringType()) \
               .add("average_rating", DoubleType()) \
               .add("rating_number", DoubleType()) \
               .add("price", StringType()) \
               .add("categories", ArrayType(ArrayType(StringType()))) \
               .add("title", StringType()) \
               .add("store", StringType())

df = spark.read.option("mergeSchema", "true") \
    .schema(schema) \
    .json("gs://de-amazon-product-review-bucket/raw/meta/*.jsonl.gz")


# select columns 
# repartition to 100 parts for processing faster
df_clean = df.select(
    col("main_category"),
    col("parent_asin"),
    col("average_rating"),
    col("rating_number"),
    col("price"),
    col("categories"),
    col("title"),
    col("store")
).repartition(100)

# store as Parquet files to gs://bucket/staging/item/
df_clean.write.mode("overwrite").parquet("gs://de-amazon-product-review-bucket/staging/test")