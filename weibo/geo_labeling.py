# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    geo_labeling.py                                    :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Xiaokang Fu <fxk123@gmail.com>             +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2023/03/11 03:28:53 by Xiaokang Fu       #+#    #+#              #
#    Updated: 2023/03/11 04:18:02 by Xiaokang Fu      ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import geopandas as gpd
import pandas as pd
import os
from tqdm import tqdm
from pandarallel import pandarallel

pandarallel.initialize(progress_bar=True)

# df.apply(func)
# df.parallel_apply(func)

china_base_map = gpd.read_file("china_city_basemap.zip")
china_base_map = china_base_map[['GbCity', 'City_EN', 'GbProv', 'Prov_EN', 'geometry']]

input_path = "../data/weibo_process/weibo_output/weibo_text_clean_parquet_daily"
out_path = "../data/weibo_process/weibo_output/weibo_text_clean_parquet_daily_city_coded"
os.makedirs(out_path, exist_ok=True)

files_df = pd.DataFrame(list(os.listdir(input_path)))
files_df.columns = ["file_name"]
files_df["file_path"] = files_df["file_name"].apply(lambda x: os.path.join(input_path, x))
files_df["out_path"] = files_df["file_name"].apply(lambda x: os.path.join(out_path, x))

def geo_labeling(file_path, out_path):

    df = pd.read_parquet(file_path)
    df = gpd.GeoDataFrame(df,
            geometry=gpd.points_from_xy(df["lon"], df["lat"]),
            crs="EPSG:4326")
    df = gpd.sjoin(df, china_base_map, how="left")
    df = df.drop(columns=["index_right"])
    df.to_parquet(out_path)

files_df.parallel_apply(lambda x: geo_labeling(x["file_path"], x["out_path"]), axis=1)

