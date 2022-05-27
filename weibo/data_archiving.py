from pymongo import MongoClient
from tqdm import tqdm,trange
import pandas as pd
import vaex
import os

def get_collection_as_dataframe(collection,collection_name):
    data_list = []
    for item in tqdm(collection.find(),total = collection.estimated_document_count(),desc = "%s"%collection_name):
        temp = {}
        # print(item)
        temp["created_at"] = item["created_at"]
        temp["id"] = item["id"]
        temp["text"] = item["text"]
        temp["pic_ids"] = ",".join(item["pic_ids"])

        try:
            temp["bmiddle_pic"] = item["bmiddle_pic"]
            # break
        except Exception as e:
            # print(e)
            temp["bmiddle_pic"] = None

        try:
            [temp["lat"],temp["lon"]] = item["geo"]["coordinates"]
        except Exception as e:
            pass
            # print(e)
            # print(item["id"])
            # break
        temp["user_id"] = item["user"]["id"]
        temp["user_location"] = item["user"]["location"]
        temp["user_gender"] =  item["user"]["gender"]
        temp["user_followers_count"] = item["user"]["followers_count"]
        temp["user_friends_count"] = item["user"]["friends_count"]
        temp["user_statuses_count"] = item["user"]["statuses_count"]
        data_list += [temp]
        # break
        
    data_list = pd.DataFrame(data_list)

    return data_list

myclient = MongoClient("mongodb://localhost:27017")

root_path = "./"
output_path1 = os.path.join(root_path,"weibp_output", "parquet")
output_path2 = os.path.join(root_path,"weibo_output", "vaex_hdf5")
os.makedirs(output_path1,exist_ok = True)
os.makedirs(output_path2,exist_ok = True)



dblist = [item for item in myclient.list_database_names() if "weibodata" in item]



for db_name in dblist:
    print("processing database %s"%db_name)
    db = myclient[db_name]
    #数据库中的集合名字
    collist = db.list_collection_names()
    # break
    for collection_name in collist:

        mycol = db[collection_name]


        data=get_collection_as_dataframe(mycol,collection_name)
        data["id"] = data["id"].astype(int)
        data["created_at"] =  pd.to_datetime(data['created_at'], errors='coerce')
        data["user_id"] =data["user_id"].astype(int)

        data.to_parquet("%s/%s.parquet"%(output_path1,collection_name),engine='pyarrow' )
        
        vaex_df = vaex.from_pandas(data, copy_index=False)  
        vaex_df.export_hdf5("%s/%s_column.hdf5"%(output_path2,collection_name))    

        # df = pd.read_parquet(file_path,engine="pyarrow")
        #存储为csv格式
        # data.to_csv("D:/jianguoyun/code/testarrow/%s.csv"%collist[i],encoding="utf-8-sig",index=False)
        # parquet格式存储，因为数据量较大，需要先转化为字符串再存储
        # test = data.astype(str)
        # data = pd.DataFrame(test)
        # table = pa.Table.from_pandas(data)
        # pq.write_table(table, 'D:/jianguoyun/code/testarrow/%s.parquet'% collist[i])
        # break
    # break
print("done!")



# database_name = "weibodata2105"
# collection_name = "China_2105_01-07"

# client = MongoClient(host="localhost",port=27017)
# collection = client[database_name][collection_name]
# data = get_collection_as_dataframe(collection)
# data.to_csv("%s.csv"%collection_name)


