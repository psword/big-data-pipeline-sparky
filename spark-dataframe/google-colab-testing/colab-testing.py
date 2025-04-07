# Philip is generally testing in Google Colab
# This reflects the code in the root datafram directory, only adapted for testing in this environment
# This assumes the CSV is uploaded...although it can be downloaded to colab from Kaggle using a user key
# At this point, the repo is not synved to colab, since it is private (manual copy/paste)

# BEGIN
# Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
spark = SparkSession.builder.appName("DataFrameTmdb").getOrCreate()

from google.colab import drive
drive.mount('/content/gdrive')

# Location of dataset
dataset = '/content/gdrive/My Drive/Colab Notebooks/TMDB_movie_dataset_v11.csv'

# Load tmdb movies dataset

# Read dataset with Spark
# Ensuring the reader reads the whole title inside quotation marks
# in case a movie title has a comma
df_movies = spark.read.csv(
    dataset,
    inferSchema=True,
    header=True,
    quote='"',
    escape='"'
)

# "Adult" column turned into boolean
df_movies = df_movies.withColumn('adult', col('adult').cast('boolean'))

# Print first 5 lines
print("First 5 movies")
df_movies.show(5)

# Print 20 first movies with revenue over 1,000,000,000 (filter where adult == False)
print("First 20 movies with revenue over 1 billion")
over_billion_revenue = df_movies.where(
    (col('revenue') > 1000000000) & (col('adult') == False)
)
over_billion_revenue.show(20)

# Print 10 movies with the highest revenue (filter where adult == False and vote_count > 1)
print("Top 10 movies with highest revenue")
highest_revenue = df_movies.where((col('adult') == False) & (col('vote_count') > 1)).orderBy(desc('revenue'))
highest_revenue.show(10)
