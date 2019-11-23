from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode

spark = SparkSession.builder.appName("Movies").getOrCreate()

raw_df = (
    spark.read.format("csv")
    .option("header", "true")
    .load("data/ml-latest-small/movies.csv")
)

transformed_df = raw_df.select(
    col("movieId"), explode(split(col("genres"), "\\|")).alias("genre")
)

transformed_df.write.mode("overwrite").parquet("data/output/movies_genres")
