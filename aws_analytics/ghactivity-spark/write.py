

def to_file(df, tgt_dir, file_format):
    df_partitioned = df.coalesce(14).\
        write.\
        partitionBy("year", 'month', 'day').\
        mode("append").\
        format(file_format).\
        save(tgt_dir)

