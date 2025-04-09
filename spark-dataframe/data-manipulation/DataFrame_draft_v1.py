import requests
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrameLab").getOrCreate()

# Download CSV-file
url = 'https://www.dropbox.com/scl/fi/a2ic1uv3j52z8k2g8u4eo/TMDB_movie_dataset_v11.csv?rlkey=i0me0oq07ecscq1nxwmwhcvjv&st=czq2g77s&dl=1'
response = requests.get(url)

with open('TMDB_movie_dataset_v11.csv', 'wb') as file:
    file.write(response.content)

# Read dataset with Spark
df_movies = spark.read.csv(
    'TMDB_movie_dataset_v11.csv',
    inferSchema=True,
    header=True
)

# Print first 5 lines
df_movies.show(5)
