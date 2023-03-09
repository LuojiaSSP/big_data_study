import os
from tqdm import tqdm
import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

tqdm.pandas()
def convert_to_fastparquet(row,output_path):
    df = pd.read_parquet(row["file_path"],engine="pyarrow")
    df.to_parquet("%s/%s.parquet"%(output_path,row["file_name"]),engine='fastparquet' )



orginal_parquest_path = "/home/ubuntu/Downloads/148638908_fxk_123/origin_parquet/"
output_path = "/home/ubuntu/xiaokang/big_data_study/data/weibo_process/weibo_output/fastparquet/"
os.makedirs(output_path,exist_ok = True)

file_list = []
for file in os.listdir(orginal_parquest_path):
    if file.endswith(".parquet"):
        file_path = os.path.join(orginal_parquest_path,file)
        file_list.append([file_path,file])
        # df = pd.read_parquet(file_path,engine="pyarrow")
        # df.to_parquet("%s/%s.parquet"%(output_path,file),engine='fastparquet' )
file_list_df = pd.DataFrame(file_list,columns = ["file_path","file_name"])


file_list_df.progress_apply(lambda row:convert_to_fastparquet(row,output_path),axis = 1)

