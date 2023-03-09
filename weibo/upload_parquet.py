import os 
from tqdm import tqdm

local_path = "/home/ubuntu/xiaokang/big_data_study/data/weibo_process/weibo_output/parquet"

for file in tqdm(os.listdir(local_path),total = len(os.listdir(local_path))):
    if file.endswith(".parquet"):
        file_path = os.path.join(local_path,file)
        print(file_path)
        cmd = """BaiduPCS-Go upload %s /data/China_weibo_data/parquet/origin_parquet/ -p 3"""%file_path
        os.system(cmd)
        print("upload %s done"%file_path)