from pyspark.sql.functions import col, expr


def _extract_data(spark, config):
    """ Load data from csv file """
    return (
        spark.read.format("csv")
        .option("header", "true")
        .load(f"{config.get('source_data_path')}/movies.csv")
    )


def _transform_data(raw_df):
    """ Transform raw dataframe """
    return raw_df.select(
        col("movieId"),
        expr("substring(title, 1, length(title)-6)").alias("title"),
        col("title").substr(-5, 4).alias("year"),
    )


def _load_data(config, transformed_df):
    """ Save data to parquet file """
    transformed_df.write.mode("overwrite").parquet(
        f"{config.get('output_data_path')}/movies"
    )


def run_job(spark, config):
    """ Run movies job """
    _load_data(config, _transform_data(_extract_data(spark, config)))
