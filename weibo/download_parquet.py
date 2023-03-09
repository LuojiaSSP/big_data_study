import os
from tqdm import tqdm

local_path = "/home/ubuntu/xiaokang/big_data_study/data/weibo_process/"

cmd = """BaiduPCS-Go download /data/China_weibo_data/parquet/origin_parquet %s  -p 3"""%local_path
os.system(cmd)
print("upload %s done"%local_path)