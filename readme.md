# 说明

weibo 目录下的脚本

- `auto_process_archiving.py` 自动从百度云下载并归档微博数据，生成parquet文件 (Xiaokang) 在上传数据之前进行处理并备份！
- `data_clean.py` 对微博数据中的文本进行清理以方便进行进一步的语言处理，为模型做准备 (Xiaokang and Dongyang)，这个主要有利于对文本进行训练等
- `drop_duplicates.py` 对微博所有的数据根据`id`进行去重并生成按照天组织的parquet文件 (Xiaokang)可以减少每次读到内存的数据大小。
- `geo_labeling.py` 对微博数据进行地理编码，生成地理编码的结果 (Xiaokang) 

以上可以产生数据的四个版本

注意 ⚠️:

上传和下载数据到百度云盘后，一定要检查一下上传和下载的数据的文件大小是否为0.上传和下载可能存在一些错误。


## Todo

- [x] 对所有的数据进行地理编码的代码
- [ ] 数据库的备份请使用脚本自动运行从而保持名字的一致性、数据格式的一致性
- [ ] Setup automaticlly tasks
- [ ] 对所有数据进行情感计算的结果
- [ ] 对所有数据进行主题计算的结果

以上这些既是代码又是数据处理的结果


## auto_process_archiving

自动从百度云下载数据并归档数据

### 功能说明
`auto_process_archiving.py` 执行下面的功能：
1. 自动从百度云下载按月压缩的7zip文件
2. 并进行解压缩
3. 上传到本地的mongo数据库
4. 并执行数据抽取、转换、结果存储为两种列存储的表格数据格式


目前提取的数据属性包括：

```bash
created_at              datetime64[ns, pytz.FixedOffset(480)]
id                                                      int64
text                                                   object
pic_ids                                                object
bmiddle_pic                                            object
lat                                                   float64
lon                                                   float64
user_id                                                 int64
user_location                                          object
user_gender                                            object
user_followers_count                                    int64
user_friends_count                                      int64
user_statuses_count                                     int64
use_lang                                               object
```



### 环境配置

1. mongo 安装、服务开启

安装

```bash
conda install mongodb
conda install mongo-tools
```

服务开启

```bash
mongod --dir you_storage_path
```

2. python、与环境配置

```bash
conda install vaex tqdm pandas pymongo tables
```

3. 百度云命令行客户端下载与配置
   
参考[GitHub - qjfoidnh/BaiduPCS-Go: iikira/BaiduPCS-Go原版基础上集成了分享链接/秒传链接转存功能](https://github.com/qjfoidnh/BaiduPCS-Go)

下载文件解压缩后，将其移动到`/usr/local/bin`以便程序能够从容易目录访问

执行登陆



4. 运行文件

修改下面的文件

```python
# 待处理的文件时间
start_date = datetime.date(2021,12,1)
end_date = datetime.date(2022,4,1)
# 百度云的待处理文件存储位置
could_base_path = "/data/China_weibo_data/mongo_data_backup/"
# 文件的下载位置以及文件处理后数据的存储位置
download_path = "/Users/kang/Downloads/weibo_process/"
```