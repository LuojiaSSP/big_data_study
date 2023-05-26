import pandas as pd
import os
from tqdm import tqdm

inpath = "../../../china_weibo_sentiment_bert_parquet_daily_removed_outlies/"
outpath = "../../../china_weibo_sentiment_bert_parquet_hourly_removed_outlies/"

for file_name in tqdm(os.listdir(inpath)):
    if "baidu" in file_name:
        continue
    data = pd.read_parquet(inpath + file_name)
    data["hour"] = data.created_at.dt.hour
    for group in data.groupby("hour"):
        hour = group[0]
        day = group[1].date.unique()[0]
        data_hour = group[1]
        data_hour.to_parquet(outpath +"weibo_China_bert_" +day.isoformat()+ "-"  +str(hour) + ".parquet")
        # print(hour)
        # print(data_hour.shape)
        # print("\n")
    # break
