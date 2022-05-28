
## 自动从百度云下载数据并归档数据

### 功能说明
`auto_process_archiving.py` 执行下面的功能：
1. 自动从百度云下载按月压缩的7zip文件
2. 并进行解压缩
3. 上传到本地的mongo数据库
4. 并执行数据抽取、转换、结果存储为两种列存储的表格数据格式

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

3. 运行文件

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