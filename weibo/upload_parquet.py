import os 
from tqdm import tqdm

# local_path = "/home/ubuntu/xiaokang/big_data_study/data/weibo_process/weibo_output/parquet"
# for file in tqdm(os.listdir(local_path),total = len(os.listdir(local_path))):
#     if file.endswith(".parquet"):
#         file_path = os.path.join(local_path,file)
#         print(file_path)
#         cmd = """BaiduPCS-Go upload %s /apps/bypy/data/weibo_data/parquet/origin_parquet/ -p 3"""%file_path
#         os.system(cmd)
#         print("upload %s done"%file_path)

# upload daily parquet
# use this upload batch files with the same prefix
# BaiduPCS-Go upload /home/ubuntu/xiaokang/big_data_study/data/weibo_process/weibo_output/weibo_text_clean_parquet_daily/weibo_text_clean_China_2023-02-* /apps/bypy/data/weibo_data/parquet/weibo_text_clean_parquet_daily/ -p 3
# local_path = "/home/ubuntu/xiaokang/big_data_study/data/weibo_process/weibo_output/weibo_text_clean_parquet_daily"
# for file in tqdm(os.listdir(local_path),total = len(os.listdir(local_path))):
#     if file.endswith(".parquet"):
#         file_path = os.path.join(local_path,file)
#         print(file_path)
#         cmd = """BaiduPCS-Go upload %s /apps/bypy/data/weibo_data/parquet/weibo_text_clean_parquet_daily/ -p 3"""%file_path
#         print(cmd)
#         # os.system(cmd)
#         # print("upload %s done"%file_path)
#         break

# upload geo_coded_city parquet
local_path = "/home/ubuntu/xiaokang/big_data_study/data/weibo_process/weibo_output/weibo_text_clean_parquet_daily_city_coded"
file_list = list(os.listdir(local_path))
# file_list = ["weibo_text_clean_China_2022-11-08.parquet"]
for file in tqdm(file_list):
    if file.endswith(".parquet"):
        file_path = os.path.join(local_path,file)
        # print(file_path)
        cmd = """BaiduPCS-Go upload %s /apps/bypy/data/weibo_data/parquet/weibo_text_clean_parquet_daily_city_coded/"""%file_path
        print(cmd)
        break
        os.system(cmd)
        print("upload %s done"%file_path)
