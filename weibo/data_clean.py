
# may need python3.6 to install all the package
import json
import re
from harvesttext import HarvestText
import pyhanlp
from loguru import logger
import pandas as pd
import tqdm
# from multiprocesspandas import applyparallel

# import vaex


def clean(text):
    text = re.sub(r"(回复)?(//)?\s*@\S*?\s*(:| |$)","", text)  # 去除正文中的@和回复/转发中的用户名
    text = re.sub(r"#\S+#","", text)      # 保留话题内容
    URL_REGEX = re.compile(
        r'(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?«»“”‘’]))',
        re.IGNORECASE)
    text = re.sub(URL_REGEX, "", text)       # 去除网址
    text = text.replace('http', '')
    text=text.replace('分享图片', '')
    text = text.replace('分享视频', '')
    text = text.replace("转发微博","")       # 去除无意义的词语
    text = re.sub(r"\s+", "", text) # 合并正文中过多的空格
    text = text.replace('\u200b', '')#去除不可见字符\u200d \U0001fac0 \U0001f964 http  \U0001fa74 \U0001f99a
    return text.strip()#保证字符串尾部没有多余空格
#去除链接
def deletehttp(sentence):
    sentence = sentence.split(r"http://t.cn/")[0]
    return sentence
#移除所有不可见字符，除\n外
def remove_invisible_chars(s):
    str = ''
    for x in s:
        if x is not '\n' and not x.isprintable():
            str += ''
        else:
            str += x
    return str

def remove_url(src):
    vTEXT = re.sub(r'(https|http)?:\/\/(\w|\.|\/|\?|\=|\&|\%)*\b', '', src, flags=re.MULTILINE)
    return vTEXT
#去除停用词
# 创建停用词
# import jieba
# def stopwordslist(filepath):
#     stopwords = [line.strip() for line in open(filepath, 'r', encoding='utf-8').readlines()]
#     return stopwords
# # 对句子进行分词
# def seg_sentence(sentence):
#     f_sw = 'D:/wdy/stopwords-master/stopwords-master/hit_stopwords.txt'
#     sentence_seged = jieba.cut(sentence.strip())
#     stopwords = stopwordslist(f_sw)
#     outstr = ''
#     for word in sentence_seged:
#         if word not in stopwords:
#             if word != '\t':
#                 outstr += word
#                 outstr += " "
#     return outstr

# # 若为字母，使用空格代替
# def deletewords(info):
#     for i in info:
#         if i.isalpha():
#             info = info.replace(i, " ")
#     return info

def clean_a_test(row):

    text = row['text']
    ht = HarvestText()
    CharTable = pyhanlp.JClass('com.hankcs.hanlp.dictionary.other.CharTable')
    num_null = 0
    cleaned_data = []
    log_path = "./log.txt"
    logger.add(log_path, rotation='1 week', retention='30 days', enqueue=True)
    # logger.info(f'old:{len(data)}\n')
    #print("old:",len(data))
    content = CharTable.convert(text)
    cleaned_content = remove_url(ht.clean_text(content, emoji=False))  # 过滤@后最多6个字符
    cleaned_content=clean(cleaned_content)
    cleaned_content=remove_invisible_chars(cleaned_content)
    cleaned_content = deletehttp(cleaned_content)
    # cleaned_content = deletewords(cleaned_content)

    return cleaned_content
  
  

def clean_text(data):
    ht = HarvestText()
    CharTable = pyhanlp.JClass('com.hankcs.hanlp.dictionary.other.CharTable')
    num_null = 0
    cleaned_data = []
    log_path = "./log.txt"
    logger.add(log_path, rotation='1 week', retention='30 days', enqueue=True)
    logger.info(f'old:{len(data)}\n')
    #print("old:",len(data))
    for i in range(len(data)):
        content = CharTable.convert(data[i]['content'])
        cleaned_content = remove_url(ht.clean_text(content, emoji=False))  # 过滤@后最多6个字符
        cleaned_content=clean(cleaned_content)
        cleaned_content=remove_invisible_chars(cleaned_content)
        cleaned_content = deletehttp(cleaned_content)
        # cleaned_content = deletewords(cleaned_content)
        data[i]['content']=cleaned_content
        num_null += 1 if cleaned_content == '' else 0
        if not content or not cleaned_content or len(cleaned_content)<=2 or cleaned_content.isnumeric():  # 删除train中的自带的空数据或清洗后出现的空数据||数据长度小于2
           cleaned_data.append(data[i])
           continue
    # print('null data num: ', num_null)
    logger.info(f'null data num:{num_null}\n')
    for j in range(len(cleaned_data)):
        data.remove(cleaned_data[j])
        print(j)
   # print("new:",len(data))
    logger.info(f'new:{len(data)}\n')
    return data


def read_json(file):
    with open(file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print('%s -> data over' % file)
    return data
def save_json(data, file):
    with open(file, 'w', encoding='utf-8') as f:
        f.write(json.dumps(data, indent=1, ensure_ascii=False))
    print('data -> %s over' % file)

if __name__ == "__main__":
    # print("test")
    input_path = "data"
    output_path = "weibo_text_clean_parquet"

    file_name = "China_2105_01-07.parquet"
    data  = pd.read_parquet('%s/%s'%(input_path,file_name))
    sample = data.sample(100)


    # text = sample['text'].values[1]
    # text_clean = clean_a_test(text)
    # print(text,"\n",text_clean)
    tqdm.tqdm.pandas(desc=file_name)
    # sample.text.apply_parallel(clean_a_test, num_processes=6)
# 369 ms ± 70.4 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)
    sample['text_clean'] = sample.progress_apply(clean_a_test,axis=1)
    # sample['text_clean'] = sample.apply(clean_a_test,axis=1)
    # 462 ms ± 36.3 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)
    sample.to_parquet("%s/%s"%(output_path,file_name))
    

