import os
import shutil
import pandas as pd
from jobs import movies


class TestMoviesJob:
    def test_transform_data(self, spark_session):
        test_data = spark_session.createDataFrame(
            [(1, "Toy Story (1995)", "Adventure"), (160646, "Goat (2016)", "Drama")],
            ["movieId", "title", "genres"],
        )

        expected_data = spark_session.createDataFrame(
            [(1, "Toy Story", 1995), (160646, "Goat", 2016)],
            ["movieId", "title", "year"],
        ).toPandas()

        real_data = movies._transform_data(test_data).toPandas()

        pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=False)

    def test_run_job(self, spark_session, mocker):
        test_config = {"output_data_path": "test_data_output"}
        shutil.rmtree(test_config.get("output_data_path"))
        test_data = spark_session.createDataFrame(
            [(1, "Toy Story (1995)", "Adventure"), (160646, "Goat (2016)", "Drama")],
            ["movieId", "title", "genres"],
        )
        mocker.patch.object(movies, "_extract_data")
        movies._extract_data.return_value = test_data
        movies.run_job(spark_session, test_config)
        assert os.path.exists(test_config.get("output_data_path"))
