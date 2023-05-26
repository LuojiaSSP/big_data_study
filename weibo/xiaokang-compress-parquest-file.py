import pandas as pd
import os
from tqdm import tqdm
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

data_path = "/n/holyscratch01/cga/xiaokang/weibo_sentiment_new/data/weibo_text_clean_daily_city_coded_sentiment_socore_bert_new/"
out_path = "/n/holyscratch01/cga/xiaokang/weibo_sentiment_new/data/weibo_text_clean_daily_city_coded_sentiment_socore_bert_new_compressed/"
os.makedirs(out_path,exist_ok=True)
need_process_files = [file for file in os.listdir(data_path) if file +".br" not in list(os.listdir(out_path))]

batches = pd.DataFrame({"file":need_process_files})
print(batches)
def compress_parquet(file):
    df = pd.read_parquet(data_path+file)
    df.to_parquet(out_path+file+".br",compression='brotli')

batches.parallel_apply(lambda x: compress_parquet(x["file"]),axis=1)

print("Done!")