from parse import parse
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

TITLE_COLUMN_TEMPLATE = "{title} ({year})"


def _get_movie_title(title_column):
    parsed = parse(TITLE_COLUMN_TEMPLATE, title_column)
    return parsed.named.get("title") if parsed else None


def _get_movie_year(title_column):
    parsed = parse(TITLE_COLUMN_TEMPLATE, title_column)
    year = parsed.named.get("year") if parsed else None
    return int(year) if year and year.isdigit() else None


get_movie_title_udf = udf(_get_movie_title, StringType())
get_movie_year_udf = udf(_get_movie_year, IntegerType())
