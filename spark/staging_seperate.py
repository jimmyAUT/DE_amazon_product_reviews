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

categories = ['All_Beauty', 'Amazon_Fashion', 'Appliances', 'Arts_Crafts_and_Sewing', 'Automotive', 
'Baby_Products', 'Beauty_and_Personal_Care', 'Books', 'CDs_and_Vinyl', 'Cell_Phones_and_Accessories', 
'Clothing_Shoes_and_Jewelry', 'Digital_Music', 'Electronics', 'Gift_Cards', 'Grocery_and_Gourmet_Food', 
'Handmade_Products', 'Health_and_Household', 'Health_and_Personal_Care', 'Home_and_Kitchen', 
'Industrial_and_Scientific', 'Kindle_Store', 'Magazine_Subscriptions', 'Movies_and_TV', 
'Musical_Instruments', 'Office_Products', 'Patio_Lawn_and_Garden', 'Pet_Supplies', 'Software', 
'Sports_and_Outdoors', 'Subscription_Boxes', 'Tools_and_Home_Improvement', 'Toys_and_Games', 
'Video_Games', 'Unknown']

for c in categories:

    print(f"Processing category: {c}")

    df = spark.read.option("mergeSchema", "true") \
        .schema(schema) \
        .json(f"gs://de-amazon-product-review-bucket/raw/meta/{c}.jsonl.gz")


    # select columns 
    df_clean = df.select(
        col("main_category"),
        col("parent_asin"),
        col("average_rating"),
        col("rating_number"),
        col("price"),
        col("categories"),
        col("title"),
        col("store")
    )

    # 存成 Parquet 檔到 staging/item/
    df_clean.write.mode("overwrite").parquet(f"gs://de-amazon-product-review-bucket/staging/meta_staing/{c}")