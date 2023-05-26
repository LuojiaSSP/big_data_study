
python /home/ubuntu/xiaokang/big_data_study/weibo/auto_process_archiving.py

/home/ubuntu/mambaforge/envs/py36/bin/python /home/ubuntu/xiaokang/big_data_study/weibo/data_clean.py

python /home/ubuntu/xiaokang/big_data_study/weibo/drop_duplicates.py

/home/ubuntu/mambaforge/envs/geopandas/bin/python /home/ubuntu/xiaokang/big_data_study/weibo/geo_labeling.py

python upload_parquet.py