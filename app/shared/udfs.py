from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType


def _get_movie_title(title_column):
    return title_column[0:-7]


def _get_movie_year(title_column):
    return int(title_column[-5:-1])


get_movie_title_udf = udf(_get_movie_title, StringType())
get_movie_year_udf = udf(_get_movie_year, IntegerType())
