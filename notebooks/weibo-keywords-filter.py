import pandas as pd
import os 
import tqdm
import glob

path = "/home/ubuntu/xiaokang/big_data_study/data/weibo_process/weibo_output/weibo_text_clean_parquet_daily_city_coded"
file_list = glob.glob(path+"/weibo_text_clean_China_2023*.parquet")
print(len(file_list))

data = pd.DataFrame()
for file in tqdm.tqdm(file_list):
    df = pd.read_parquet(file)
    data = pd.concat([data,df])

# 俄亥俄
# 化学品泄漏
# 有毒气泄漏
# 毒气体泄漏
# 氯乙烯泄漏
# 毒火车事件
# 毒火车
key_words = ["俄亥俄","化学品泄漏","有毒气泄漏","毒气体泄漏","氯乙烯泄漏","毒火车事件","毒火车"]
results = data[data.text.str.contains("|".join(key_words))]
