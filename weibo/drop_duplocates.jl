using DataFrames, ProgressMeter
using Dates
using StatsBase

# drop duplicates
output_dir = "../data/weibo_process/weibo_output/weibo_text_clean_parquet_daily/"
# mkdir(output_dir, exist_ok = true)
input_dir = "../data/weibo_process/weibo_output/weibo_text_clean_parquet/"

for item in progress(readdir(input_dir))
    
    df = DataFrame(load(input_dir * item))
    df[!, "date"] = Date.(df.created_at)
    groups = groupby(df, :date)

    for (k, group) in progress(groups)
        if k < Date(2022, 4, 30)
            continue
        end
        file_name = "weibo_text_clean_China_" * string(k) * ".parquet"
        path = joinpath(output_dir, file_name)
        if isfile(path)
            pre = DataFrame(load(path))
            nhave = group
            com = vcat(pre, nhave)
            com = unique(com, [:id])
            # save(path, com)
        else
            continue
            # save(path, group)
        end
    end
end
