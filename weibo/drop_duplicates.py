# 去重, 
# 这里也把数据集拆分成按照天数的数据集
# drop duplicates
import pandas as pd
from tqdm import tqdm
from datetime import date
import os

# 修改这里的路径
output_dir = "../data/weibo_process/weibo_output/weibo_text_clean_parquet_daily/"
os.makedirs(output_dir,exist_ok=True)
input_dir = "../data/weibo_process/weibo_output/weibo_text_clean_parquet/"

for item in tqdm(os.listdir(input_dir)):
    
    df = pd.read_parquet(input_dir+item)
    df["date"] = df.created_at.dt.date
    groups = df.groupby("date")
    
    for k in tqdm(groups.groups,total=groups.ngroups):
        if  k < date(2022,4,30):
            continue
        file_name = "weibo_text_clean_China_"+k.isoformat()+".parquet"
    #     print(file_name)
        path = '%s%s'%(output_dir,file_name)
    #     print(path)
        group = groups.get_group(k)
        if file_name in list(os.listdir(output_dir)):
            pre = pd.read_parquet(path)
            nhave = group
            com = pd.concat([pre,nhave])
            com.drop_duplicates(subset=["id"],inplace=True)
            com.to_parquet(path)
        else:
            group.to_parquet(path)
    #     break