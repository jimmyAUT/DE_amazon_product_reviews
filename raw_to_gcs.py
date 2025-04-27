import os
import subprocess
import time
import logging


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


service_account_key = os.environ["GCP_CREDENTIALS"]

# set up gsutil CREDENTIALS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key

review_base_url = 'https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/'
meta_base_url = 'https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/meta_categories/meta_'

categories = ['All_Beauty', 'Amazon_Fashion', 'Appliances', 'Arts_Crafts_and_Sewing', 'Automotive', 
'Baby_Products', 'Beauty_and_Personal_Care', 'Books', 'CDs_and_Vinyl', 'Cell_Phones_and_Accessories', 
'Clothing_Shoes_and_Jewelry', 'Digital_Music', 'Electronics', 'Gift_Cards', 'Grocery_and_Gourmet_Food', 
'Handmade_Products', 'Health_and_Household', 'Health_and_Personal_Care', 'Home_and_Kitchen', 
'Industrial_and_Scientific', 'Kindle_Store', 'Magazine_Subscriptions', 'Movies_and_TV', 
'Musical_Instruments', 'Office_Products', 'Patio_Lawn_and_Garden', 'Pet_Supplies', 'Software', 
'Sports_and_Outdoors', 'Subscription_Boxes', 'Tools_and_Home_Improvement', 'Toys_and_Games', 
'Video_Games', 'Unknown']


bucket_name = "de-amazon-product-review-bucket"
review_prefix = "raw/reviews"
meta_prefix = "raw/meta"

def stream_upload(url: str, gcs_path: str, max_retries=3):
    """ wget stream gsutil upload raw data from URL straight to GCS"""
    cmd = f"wget -q -O - '{url}' | gsutil cp - '{gcs_path}'"
    for attempt in range(1, max_retries + 1):
        try:
            logging.info(f"⬆️ uploading: {url} ➜ {gcs_path} (attempt times: {attempt} )")
            subprocess.run(cmd, shell=True, check=True)
            logging.info("✅ Successful")
            break
        except subprocess.CalledProcessError as e:
            logging.warning(f"⚠️ Upload fail ( attempt times: {attempt} ): {e}")
            if attempt < max_retries:
                time.sleep(2 * attempt)
            else:
                logging.error(f"❌ Upload fail excess {max_retries} ：{url}")

def main():
    for category in categories:
        review_url = f"{review_base_url}{category}.jsonl.gz"
        meta_url = f"{meta_base_url}{category}.jsonl.gz"

        review_gcs_path = f"gs://{bucket_name}/{review_prefix}/{category}.jsonl.gz"
        meta_gcs_path = f"gs://{bucket_name}/{meta_prefix}/{category}.jsonl.gz"

        stream_upload(review_url, review_gcs_path)
        stream_upload(meta_url, meta_gcs_path)

if __name__ == "__main__":
    main()
    