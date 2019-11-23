from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

spark = SparkSession.builder.appName("Movies").getOrCreate()

raw_df = (
    spark.read.format("csv")
    .option("header", "true")
    .load("data/ml-latest-small/movies.csv")
)

transformed_df = raw_df.select(
    col("movieId"),
    expr("substring(title, 1, length(title)-6)").alias("title"),
    col("title").substr(-5, 4).alias("year"),
)

transformed_df.write.mode("overwrite").parquet("data/output/movies")
