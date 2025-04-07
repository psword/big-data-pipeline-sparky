import os.path
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, desc, round, date_format, format_number, regexp_replace
from pyspark.sql.types import StringType

# Convert minutes to hours and minutes
def convert_to_hours_minutes(runtime):
    if runtime is not None:
        hours, minutes = divmod(runtime, 60)
        return f'{hours}:{minutes:02d}'
    return None

# Define UDF
convert_udf = udf(convert_to_hours_minutes, StringType())

spark = SparkSession.builder.appName("TMDB_movies").getOrCreate()

# Download CSV-file if it doesn't exist in folder
url = 'https://www.dropbox.com/scl/fi/a2ic1uv3j52z8k2g8u4eo/TMDB_movie_dataset_v11.csv?rlkey=i0me0oq07ecscq1nxwmwhcvjv&st=czq2g77s&dl=1'
dataset = 'TMDB_movie_dataset_v11.csv'

if not os.path.exists(dataset):
    import requests
    response = requests.get(url)
    with open(dataset, 'wb') as file:
        file.write(response.content)

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

# Turn "adult" column into boolean
df_movies = df_movies.withColumn('adult', col('adult').cast('boolean'))

# Round average vote score to 2 decimals
df_movies = df_movies.withColumn('vote_average2', round(df_movies.vote_average, 2))

# Reformat release date and runtime
df_movies = df_movies.withColumn('release_date', date_format('release_date', 'dd/MM/yyyy'))
df_movies = df_movies.withColumn('runtime', convert_udf(col('runtime')))

# Reorder columns
df_movies = df_movies.select('title', 'vote_average2', 'release_date', 'revenue',
                            'budget', 'runtime', 'genres', 'production_companies')

# Rename columns
df_movies = df_movies.withColumnRenamed('title', 'Title') \
    .withColumnRenamed('vote_average2', 'Average Vote Score') \
    .withColumnRenamed('release_date', 'Release Date') \
    .withColumnRenamed('revenue', 'Revenue') \
    .withColumnRenamed('budget', 'Budget') \
    .withColumnRenamed('runtime', 'Length') \
    .withColumnRenamed('genres', 'Genres') \
    .withColumnRenamed('production_companies', 'Production Companies')

# Drop unneeded columns
df_movies = df_movies.drop('id', 'vote_average', 'vote_count', 'status', 'adult',
                'backdrop_path', 'homepage', 'imdb_id', 'original_language',
                'original_title', 'overview', 'popularity', 'poster_path',
                'tagline', 'production_countries', 'spoken_languages', 'keywords')

# Print first 5 lines
# Reformat numbers
print('First 5 movies')
first_five = (
    df_movies
    .withColumn('Revenue', format_number(col('Revenue').cast('double'), 0))
    .withColumn('Budget', format_number(col('Budget').cast('double'), 0))
)
# Replace commas with spaces
first_five = (
    first_five
    .withColumn('Revenue', regexp_replace('Revenue', ',', ' '))
    .withColumn('Budget', regexp_replace('Budget', ',', ' '))
)
first_five.show(5, truncate = False)

# Print 20 first movies with revenue over 1,000,000,000
# Filter where adult == False
# Reformat numbers and rename columns
print('First 20 movies with revenue over 1 billion')
over_billion_revenue = (
    df_movies
    .where((col('Revenue') > 1000000000) & (col('adult') == False))
    .withColumn('Revenue', format_number(col('Revenue').cast('double'), 0))
    .withColumn('Budget', format_number(col('Budget').cast('double'), 0))
)
# Replace commas with spaces
over_billion_revenue = (
    over_billion_revenue
    .withColumn('Revenue', regexp_replace('Revenue', ',', ' '))
    .withColumn('Budget', regexp_replace('Budget', ',', ' '))
)
over_billion_revenue.show(20, truncate = False)

# Print 10 movies with the highest revenue
# Filter where adult == False and vote_count > 1
# Reformat numbers and rename columns
print('Top 10 movies with highest revenue')
highest_revenue = (
    df_movies
    .where((col('adult') == False) & (col('vote_count') > 1))
    .orderBy(desc('Revenue'))
    .withColumn('Revenue', format_number(col('Revenue').cast('double'), 0))
    .withColumn('Budget', format_number(col('Budget').cast('double'), 0))
)
# Replace commas with spaces
highest_revenue = (
    highest_revenue
    .withColumn('Revenue', regexp_replace('Revenue', ',', ' '))
    .withColumn('Budget', regexp_replace('Budget', ',', ' '))
)
highest_revenue.show(10, truncate = False)