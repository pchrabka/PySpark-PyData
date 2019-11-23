import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode

with open("config.json", "r") as config_file:
    config = json.load(config_file)

spark = SparkSession.builder.appName(config.get("app_name")).getOrCreate()

raw_df = (
    spark.read.format("csv")
    .option("header", "true")
    .load(f"{config.get('source_data_path')}/movies.csv")
)

transformed_df = raw_df.select(
    col("movieId"), explode(split(col("genres"), "\\|")).alias("genre")
)

transformed_df.write.mode("overwrite").parquet(
    f"{config.get('output_data_path')}/movies_genres"
)
