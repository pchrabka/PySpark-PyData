import requests
import zipfile
import io

zip_file_url = "http://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
output_path = "data/"

response = requests.get(zip_file_url)

if response.status_code == 200:
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    zip_file.extractall(output_path)
