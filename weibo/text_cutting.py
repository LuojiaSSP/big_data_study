import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)
import jieba
import os

def seg_sentence_jieba(sentence,stopwords):
    sentence_seged = jieba.cut(sentence.strip())
    sentence_seged = [word for word in sentence_seged if ((word not in stopwords) & (word != '\t'))]
    outstr = ' '.join(sentence_seged)
    return outstr


stopwords_list = []
with open('./stopwords/baidu_stopwords.txt', 'r', encoding='utf-8') as f:
    stopwords = f.readlines()
    stopwords = [x.strip() for x in stopwords if x.strip() != '']
    stopwords_list += stopwords
with open('./stopwords/hit_stopwords.txt', 'r', encoding='utf-8') as f:
    stopwords += f.readlines()
    stopwords = [x.strip() for x in stopwords if x.strip() != '']
    stopwords_list += stopwords
with open('./stopwords/scu_stopwords.txt', 'r', encoding='utf-8') as f:
    stopwords += f.readlines()
    stopwords = [x.strip() for x in stopwords if x.strip() != '']
    stopwords_list += stopwords
with open('./stopwords/cn_stopwords.txt', 'r', encoding='utf-8') as f:
    stopwords += f.readlines()
    stopwords = [x.strip() for x in stopwords if x.strip() != '']
    stopwords_list += stopwords

def seg_one_file(file,data_path,out_path,stopwords_list):
    try:
        df = pd.read_parquet(data_path+file)
        # print(df.columns)
        df.loc[:,"text_clean_cutted"] = df.apply(lambda x: seg_sentence_jieba(x["text_clean"],stopwords_list),axis=1)
        df.to_parquet(out_path+file,compression="brotli")
    except Exception as e:
        print(e)
        print(file)


data_path = "/n/holyscratch01/cga/xiaokang/weibo_sentiment_new/data/weibo_text_clean_daily_city_coded_sentiment_socore_bert_new/"


# setence = "'你好，欢迎在Python中调用HanLP的API'"
data_path = "/n/holyscratch01/cga/xiaokang/weibo_sentiment_new/data/weibo_text_clean_daily_city_coded_sentiment_socore_bert_new_compressed/"
out_path = "/n/holyscratch01/cga/xiaokang/weibo_sentiment_new/data/weibo_text_clean_daily_city_coded_sentiment_socore_bert_new_compressed_cutted/"
# print(seg_sentence_pyhanlp(setence))
os.makedirs(out_path,exist_ok=True)
need_process_files = [file for file in os.listdir(data_path) if file not in list(os.listdir(out_path))]
batches = pd.DataFrame({"file":need_process_files})

import datetime
t1 = datetime.datetime.now()
batches.parallel_apply(lambda x: seg_one_file(x["file"],data_path,out_path,stopwords_list),axis=1)
t2 = datetime.datetime.now()
print(t2-t1)
