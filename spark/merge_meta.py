from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from pyspark.sql.functions import lit
# 初始化 SparkSession
spark = SparkSession.builder.appName("MergeCategoryItems").getOrCreate()

# 類別清單（Parquet 資料夾名稱）
categories = [
    'All_Beauty', 'Amazon_Fashion', 'Appliances', 'Arts_Crafts_and_Sewing', 'Automotive', 
    'Baby_Products', 'Beauty_and_Personal_Care', 'Books', 'CDs_and_Vinyl', 'Cell_Phones_and_Accessories', 
    'Clothing_Shoes_and_Jewelry', 'Digital_Music', 'Electronics', 'Gift_Cards', 'Grocery_and_Gourmet_Food', 
    'Handmade_Products', 'Health_and_Household', 'Health_and_Personal_Care', 'Home_and_Kitchen', 
    'Industrial_and_Scientific', 'Kindle_Store', 'Magazine_Subscriptions', 'Movies_and_TV', 
    'Musical_Instruments', 'Office_Products', 'Patio_Lawn_and_Garden', 'Pet_Supplies', 'Software', 
    'Sports_and_Outdoors', 'Subscription_Boxes', 'Tools_and_Home_Improvement', 'Toys_and_Games', 
    'Video_Games', 'Unknown'
]

# 預設路徑
base_path = "gs://de-amazon-product-review-bucket/staging/meta_staing"


# 初始化空 DataFrame
df_all = None


for cat in categories:
    path = f"{base_path}/{cat}/"
    try:
        df = spark.read.parquet(path)
        
        # 避免 schema 衝突，並保留來源類別
        df = df.withColumn("source_category", lit(cat))

        print(f"✔ 成功讀取: {cat} | 筆數: {df.count()}")

        if df_all is None:
            df_all = df
        else:
            df_all = df_all.unionByName(df)

    except AnalysisException as e:
        print(f"⚠ 無法讀取: {cat} | 原因: {e}")
    except Exception as e:
        print(f"❌ 錯誤讀取: {cat} | 原因: {e}")


print("✅ 合併後總筆數:", df_all.count())
df_all.printSchema()

# 寫入最終整合表
df_all.write.mode("overwrite").parquet("gs://de-amazon-product-review-bucket/staging/item_merged/")