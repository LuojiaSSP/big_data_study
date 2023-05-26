import os
import re
import pandas as pd
from tqdm import tqdm
could_path = "/apps/bypy/data/weibo_data/parquet/weibo_text_clean_parquet_daily_city_coded/"

# get cmd output string
def get_cmd_output(cmd):
    r = os.popen(cmd)
    text = r.read()
    r.close()
    return text
cmd = """BaiduPCS-Go ls %s"""%could_path
cmd_output = get_cmd_output(cmd)
cmd_output = cmd_output.split("----")[1]
cmd_output = cmd_output.split("总")[0]
cmd_output = cmd_output.split("\n")
cmd_output = cmd_output[1:-1]
table_header = cmd_output[0]
table_header = table_header.strip(" ")
# relace all the continuous space with one comma
table_header = ",".join(table_header.split())
table_body = cmd_output[1:]
# relace all the continuous space if there are more than 2 spaces with one comma
# table_body 
table_body = [i.strip(" ") for i in table_body]
table_body = [re.sub(r'\s{2,}', ',', input_str) for input_str in table_body]
table = [table_header] + table_body
# the header is the first row
df = pd.DataFrame([i.split(",") for i in table_body],columns = table_header.split(","))

# check 0MB file
failed_df = df[df["文件大小"]=="0B"]
print(failed_df)

# for file_name in failed_df["文件(目录)"]:
#     cmd = """BaiduPCS-Go rm %s%s"""%(could_path,file_name)
#     os.system(cmd)
#     print("upload %s done"%file_name)

# local_path = "/home/ubuntu/xiaokang/big_data_study/data/weibo_process/weibo_output/weibo_text_clean_parquet_daily_city_coded"
# # file_list = list(os.listdir(local_path))
# file_list = failed_df["文件(目录)"].values
# for file in tqdm(file_list):
#     if file.endswith(".parquet"):
#         file_path = os.path.join(local_path,file)
#         # print(file_path)
#         cmd = """BaiduPCS-Go upload %s /apps/bypy/data/weibo_data/parquet/weibo_text_clean_parquet_daily_city_coded/"""%file_path
#         os.system(cmd)
#         print("upload %s done"%file_path)
