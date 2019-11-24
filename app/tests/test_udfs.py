from shared import udfs


class TestUDFs:
    def test_get_movie_title_value(self):
        test_value = "Toys Story (1995)"
        expected_value = "Toys Story"
        assert udfs._get_movie_title(test_value) == expected_value

    def test_get_movie_year_value(self):
        test_value = "Toys Story (1995)"
        expected_value = 1995
        assert udfs._get_movie_year(test_value) == expected_value
