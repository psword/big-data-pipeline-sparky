from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrameLab").getOrCreate()

df_movies = spark.read.csv(
    "TMDB_movie_dataset_v11.csv",
    inferSchema=True,
    header=True
)

df_movies.show(5)