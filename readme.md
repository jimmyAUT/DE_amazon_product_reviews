# End-to-End Data Pipeline for Amazon Product Reviews Analysis

---

## Problem statement


---

## Project Overview

This project builds an end-to-end data pipeline for Amazon Product Reviews to transform raw data into clean, structured, and trusted datasets. By designing a scalable and tested pipeline with dbt, BigQuery, and Power BI, this project enable fast, reliable, and insightful analysis of customer behaviors, product trends, and sales performance. This foundation is crucial because it empowers businesses to make data-driven decisions with confidence, optimize products and marketing strategies, and uncover hidden opportunities in massive datasets that would otherwise be too chaotic to analyze effectively.



Data source:  
## Bridging Language and Items for Retrieval and Recommendation
Yupeng Hou, Jiacheng Li, Zhankui He, An Yan, Xiusi Chen, Julian McAuley
arXiv:2403.03952, 2024

python subprocess and gcloud upload raw data from website to GCS data lake straight
gs://de-amazon-product-review-bucket/staging/reviews/        ← Parquet, partitioned by year or category
gs://de-amazon-product-review-bucket/staging/items/          ← Parquet, one row per product

staging/reviews	review_year or category	加快讀取速度
staging/items	可不分區或依 main_category	商品較少，速度影響小

SPARK process raw data clean, transformation and store in staging
main_category	分類主題，可作為維度用來分析不同商品群	✅ 保留
parent_asin	商品唯一識別碼，作為主鍵與 review 關聯	✅ 保留
average_rating	平均評分，常用於推薦或商品品質分析	✅ 保留
rating_number	評分次數，衡量受歡迎程度或樣本大小	✅ 保留
price	商品價格，可用於價位分析等	✅ 保留
categories	層級分類，雖然有時是空的，但仍具參考價值	✅ 保留
title	商品名稱，對 debugging / 驗證數據非常有幫助	✅ 建議保留
store or brand	若未來你考慮建 dim_brand、商店分析等	➕ 可選擇保留
details ➜ UPC	可作為商品的第二識別碼（條碼），補充資訊	➕ 可選擇保留

➤ 基本 Spark Job 所需參數：
總資料量：571M reviews（假設每筆 avg 1 KB，約 571 GB）

Cluster workers 記憶體大小、core 數量、shuffle 空間

Spark 通常建議每個 partition ≈ 128 MB（或 256 MB）
 每個 executor 建議配置（n1-standard-4 為例）：
vCPU：4（通常每個 executor 用 2 vCPU）

Memory：15 GB（約可分配給 executor ≈ 10 GB）

→ 可以安全地啟動約 2 executors / node
→ 如果你有 2 workers，每個 2 executors，總共跑 4 executors（最多並行 4 task）

➤ 評估方式：
如果你 Spark Job 花超過 30 分鐘以上，且 CPU/Memory 沒滿載 → partition 太少

如果你看到 task skew（某幾個 partition 特別大）→ 資料分布不均（你就有這個問題）

Partition 策略：如何切分？
你目前的 review dataset 是：

多 category

review 數不平均

每筆資料獨立（適合 Spark）

✅ 建議：
repartition by category：確保每個 category 至少分到 1 個 partition

若進一步分析某些 hot category，可再細分成 (category, year) 或 (category, asin)

避免過多 small file → 用 .coalesce() 控制輸出數量


使用 2 master + 4 workers 的標準叢集（可調整）

使用 預設的 Dataproc Image + Spark

開啟 Jupyter Notebook 和 Component Gateway，方便後續測試或使用 PySpark 互動式處理

可將此 cluster 後續連接到 GCS（存取 raw/staging）、BigQuery（結果上傳）或 Spark job template

# Note: using the terrafrom to set up dataproc cluster, it needs to set up the service account auths as "serviceAccountUser", even it assign the same service account for terraform and dataproc resource 

```bash
    gcloud projects add-iam-policy-binding <YOUR_PROJECT_ID> \
    --member="serviceAccount:<the service account for terraform.eg:terraform-sa@project-id.iam.gserviceaccount.com" \
    --role="roles/iam.serviceAccountUser" 
```


gsutil cp staging_item_data.py gs://de-amazon-product-review-bucket/scripts/

gcloud dataproc jobs submit pyspark gs://de-amazon-product-review-bucket/scripts/staging_meta_data.py \
  --cluster=amazon-review-cluster \
  --region=australia-southeast1


dataproc submit 性能調教
gcloud dataproc jobs submit pyspark gs://de-amazon-product-review-bucket/scripts/staging_meta_data.py \
  --cluster=amazon-review-cluster \
  --region=australia-southeast1 \
  --properties=spark.executor.instances=8,spark.executor.memory=4g,spark.executor.cores=2,spark.driver.memory=4g 
executor.instances=8: 合理利用 4 workers（每台 4 vCPU），分 2 cores/ executor 可跑滿。

executor.memory=4g: 預設 15 GB RAM，每 executor 保守用 4G。

driver.memory=4g: 提升 driver 記憶體，避免 metadata 太多被殺。


review jobs 調教
gcloud dataproc jobs submit pyspark gs://de-amazon-product-review-bucket/scripts/staging_review_data.py \
  --cluster=amazon-review-cluster \
  --region=australia-southeast1 \
  --properties=spark.executor.instances=4,\
spark.executor.memory=6g,\
spark.executor.cores=2,\
spark.driver.memory=4g,\
spark.sql.shuffle.partitions=200,\
spark.default.parallelism=200


mergeSchema=False
如果不同檔案之間的 schema 有些微差異（例如某些欄位在某些檔案中不存在），Spark 預設只會依照第一批讀到的檔案的 schema 來處理，忽略其餘檔案中額外存在的欄位。


df = spark.read.json("gs://.../*.jsonl.gz")
預設情況下這樣讀取 不會自動合併欄位（mergeSchema=false），所以如果第一批讀進來的檔案剛好沒有 "sort_timestamp"，而你後面用了 col("sort_timestamp")，就會報：

pyspark.sql.utils.AnalysisException: Column 'sort_timestamp' does not exist.

# 讀取原始 review 資料（啟用 mergeSchema）
df = spark.read \
    .schema(review_schema) \
    .option("mergeSchema", "true") \
    .json("gs://de-amazon-product-review-bucket/raw/reviews/*.jsonl.gz")
這樣 Spark 就會去掃描所有檔案並建立一個統一的 schema，包含所有可能出現的欄位名稱，你後面用 coalesce(col("sort_timestamp"), col("timestamp")) 就不會出錯了。

STEP4: dbt 資料建模與資料倉儲部署
                      ┌────────────┐
                      │ dim_users  │
                      └────┬───────┘
                           │
                           ▼
┌────────────┐       ┌────────────┐       ┌──────────────┐
│ dim_items  │◄──────│ fact_reviews│─────►│ dim_time     │
└────────────┘       └────────────┘       └──────────────┘
                           ▲
                           │
                     ┌─────┴──────┐
                     │ dim_asin   │ (Optional)

fact_reviews（事實表）

review_id	唯一識別碼（可以自建）
parent_asin	商品代碼
user_id	使用者 ID
rating	評分（1~5）
verified_purchase	是否為已驗證購買者
helpful_votes	幫助票數
timestamp	實際時間戳記（可分解至 dim_time）

dim_items（商品維度）
parent_asin	主商品代碼（主鍵）
title	商品名稱
main_category	主類別
store	店鋪名稱
categories	類別階層（Array，可展平）
price	價格
average_rating	商品平均評分
rating_number	商品總評論數

dim_users（使用者維度）
user_id	使用者 ID（主鍵）
...	可擴充欄位，如 review count 等

dim_time（時間維度）
timestamp	datetime
year	年份
month	月份
day	日期
week	週數
weekday	星期幾
hour	小時


可以考慮的分析問題
各類別商品的平均評分趨勢？

哪些店鋪商品最常獲得高評價？

Verified 購買者的評價是否更偏好？

哪些月份的評價量最多？（時間分析）


由GCS建立外部表給DBT建模使用
stg_items_ext
bq mkdef --source_format=PARQUET \
    gs://de-amazon-product-review-bucket/staging/item/*.parquet > stg_items_def.json

bq mk --table \
  --external_table_definition=stg_items_def.json \
  de-amazon-product-review:de_amazon_product_reviews.stg_items_ext

stg_reviews_ext
bq mkdef --source_format=PARQUET \
    gs://de-amazon-product-review-bucket/staging/review/*.parquet > stg_reviews_def.json

bq mk --table \
  --external_table_definition=stg_reviews_def.json \
  de-amazon-product-review:de_amazon_product_reviews.stg_reviews_ext


STAR SCHEMA建模
受益於 dim_time，如果希望未來分析能夠更彈性或更深入，比如：
分析週（如 2024年第36週）	✅ 是
分析季（Q1、Q2、Q3、Q4）	✅ 是
節日分析（如黑五、聖誕週、雙11等）	✅ 是（可加欄位）
判斷是否週末／平日	✅ 是
分析平年/閏年影響	✅ 是（可擴充）
與其他資料如銷售、庫存時間維度整合	✅ 是
在 BI 工具建立日期 slicer/filter	✅ 是


COUNTIF() 是 BigQuery 中有效且非常常用的聚合函數，專門用來有條件地計數


Too many partitions produced by query, allowed 4000, query produces at least 4001 partitions

意思是：「你的 fct_reviews 建表時，產生的分區超過 4000 個了」，而 BigQuery 每個 table 最多只能有 4000 個分區。資料量超過 570M 筆，如果橫跨了 11年以上（>4000天） 的話，就會爆炸。
減少分區數量」改成按「月」分區


Power BI 不支援直接讀取 BigQuery 中的 BYTES 欄位