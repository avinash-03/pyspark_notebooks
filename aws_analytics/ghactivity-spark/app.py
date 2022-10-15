from pyspark.sql import SparkSession
import os
from util import get_spark_session
from read import from_file
from process import transform_df
from write import to_file


def main():
    env = os.environ.get("ENVIRON")
    src_dir = os.environ.get("SRC_DIR")
    src_file_pattern = f"{os.environ.get('SRC_FILE_PATTERN')}*"
    src_file_format = os.environ.get("SRC_FILE_FORMAT")
    tgt_dir = os.environ.get("TGT_DIR")
    tgt_file_format = os.environ.get("TGT_FILE_FORMAT")
    spark = get_spark_session(env, "github activity")
    df = from_file(spark, src_dir, src_file_pattern, src_file_format)
    transformed_df = transform_df(df)
    to_file(transformed_df, tgt_dir, tgt_file_format)

    print('HI from spark')


if __name__ == "__main__":
    main()

