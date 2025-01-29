############################################################################################
### This file was ran locally. It can be switched to the cluster by changing the paths.  ###
### The following command was used to run this program.                                  ###
### spark-submit --driver-memory 4g --executor-memory 16g artist_popularity_migration.py ###
############################################################################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, collect_list, first, avg, median, udf, explode, \
    collect_set, size, expr, array_intersect, lit
from pyspark.sql.functions import max as max_df
from pyspark.sql.functions import min as min_df
from pyspark.sql.types import IntegerType
from datetime import datetime
from tqdm import tqdm


# Initialize Spark
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

def calculate_dataset_size(df, step_description):
    size = df.count()
    tqdm.write(f'[SUCCESS] {step_description}: Dataset size is {size}')

def max_sequential_dates_3_days_apart(dates):
    if not dates or len(dates) < 2:
        return 1

    sorted_dates = sorted(dates)
    max_streak = 1
    current_streak = 1
    for i in range(1, len(sorted_dates)):
        if (datetime.strptime(sorted_dates[i],'%Y-%m-%d') - datetime.strptime(sorted_dates[i - 1],'%Y-%m-%d')).days < 4:
            current_streak += 1
            max_streak = max(max_streak, current_streak)
        else:
            current_streak = 1
    return max_streak

max_sequential_udf = udf(max_sequential_dates_3_days_apart, IntegerType())

# Step: Load CSV file
csv_path = '../merged_data.csv'
csv_path_df1 = spark.read.csv(csv_path, header=True, inferSchema=True)

# Step: Calculate dataset size (initial)
calculate_dataset_size(csv_path_df1, 'Initial dataset loaded')

# Step: Remove viral50 records
csv_path_df2 = csv_path_df1.filter(col('chart') == 'top200')


# Step: Calculate dataset size (filtered)
calculate_dataset_size(csv_path_df2, 'Dataset after filtering viral50 records')

# Step: Remove Records released before 2017
csv_path_df2 = csv_path_df2.filter("release_date > date'2017-01-01'")

# Step: Calculate dataset size (filtered)
calculate_dataset_size(csv_path_df2, 'Dataset after filtering records released before 2017')

# Step: Select required columns
csv_path_df3 = csv_path_df2.select(col('track_id'), col('region'), col('release_date'), col('date'), col('artist'))

# Calculate the size of CSV file
calculate_dataset_size(csv_path_df3, 'Base CSV size')

# Split the artists and remove whitespace
artists_split = csv_path_df3.withColumn(
    'artists',
    expr('transform(split(artist, ","), x -> trim(x))')
)

# Group songs on track_id and region and calculate size: ~315k. Also get first date it occurred in the charts
df4 = (
    artists_split
    .withColumn('artist', explode(col('artists')))  # Flatten the artists list
    .groupBy(col('track_id'), col('region'))
    .agg(
        first('release_date').alias('release_date'),
        min_df(('date')).alias('dates'),
        collect_set('artist').alias('artists')  # Collect distinct artists
    )
)
calculate_dataset_size(df4, 'Dataset after grouping by track_id and region. Artists are grouped as a set to avoid duplicates')

# Get first date when a song has reached the top200 in a country
global_first_occurrence = df4.groupBy("track_id").agg(
    min_df(col(("dates"))).alias("first_date")  # Calculate the first date per title
).alias("agg").join(
    df4.alias("orig"),  # Alias the original DataFrame
    (col("agg.track_id") == col("orig.track_id")) & (col("agg.first_date") == col("orig.dates")),  # Disambiguate column references
    "inner"
).select(
    col("agg.track_id"),
    col("agg.first_date"),
    col("orig.region").alias("first_region")
)

# Explode artists
artist_explode_df = df4.withColumn('artist', explode(col('artists')))
calculate_dataset_size(artist_explode_df, 'Dataset after exploding artist column')

# count artist occurrences and sort them descending
artist_counts = artist_explode_df.groupBy('artist').count().orderBy(col('count').desc())

# calculate the size of splits
row_count = artist_counts.count()
split_size = row_count // 3

# Create a dataframe of all songs by top third artists
print("Creating top third")
first = artist_counts.limit(split_size)
calculate_dataset_size(first, 'Amount of artists in top third')
artists_first_arr = first.groupBy().agg(collect_list(col('artist')).alias('artists')).first()['artists']
top_songs = df4.filter(size(array_intersect(col('artists'), lit(artists_first_arr))) > 0)
calculate_dataset_size(top_songs, 'Dataset of songs from top third of artists')

# Create a dataframe of all songs by middle third artists
print("Creating middle third")
remaining = artist_counts.subtract(first).orderBy(col('count').desc())
second = remaining.limit(split_size)
calculate_dataset_size(second, 'Amount of artists in middle third')
artists_second_arr = second.groupBy().agg(collect_list(col('artist')).alias('artists')).first()['artists']
middle_songs = df4.filter(size(array_intersect(col('artists'), lit(artists_second_arr))) > 0)
calculate_dataset_size(middle_songs, 'Dataset of songs from middle third of artists')

# Create a dataframe of all songs by bottom third artists
print("Creating bottom third")
third = remaining.subtract(second).orderBy(col('count').desc())
calculate_dataset_size(third, 'Amount of artists in bottom third')
artists_third_arr = third.groupBy().agg(collect_list(col('artist')).alias('artists')).first()['artists']
bottom_songs = df4.filter(size(array_intersect(col('artists'), lit(artists_third_arr))) > 0)
calculate_dataset_size(middle_songs, 'Dataset of songs from bottom third of artists')

print("Getting migration pattern of top artists")
migrations_top = global_first_occurrence.alias("a").join(
    top_songs.alias("b"),
    (col("a.track_id") == col("b.track_id")) &  # Match title
    (col("b.dates") >= col("a.first_date")) &  # Same or later date
    (col("a.first_region") != col("b.region")),  # Different region
    "inner"
).select(
    col("a.track_id"),
    col("a.first_region").alias("from_region"),
    col("b.region").alias("to_region"),
    col("a.first_date").alias("from_date"),
    col("b.dates").alias("to_date")
)

# Remove duplicates if a song has been first charted in multiple region before the to_region
migrations_top = migrations_top.dropDuplicates(['track_id', 'to_region'])

print("Getting migration pattern of middle artists")
migrations_middle = global_first_occurrence.alias("a").join(
    middle_songs.alias("b"),
    (col("a.track_id") == col("b.track_id")) &  # Match title
    (col("b.dates") >= col("a.first_date")) &  # Same or later date
    (col("a.first_region") != col("b.region")),  # Different region
    "inner"
).select(
    col("a.track_id"),
    col("a.first_region").alias("from_region"),
    col("b.region").alias("to_region"),
    col("a.first_date").alias("from_date"),
    col("b.dates").alias("to_date")
)

migrations_middle = migrations_middle.dropDuplicates(['track_id', 'to_region'])

print("Getting migration pattern of bottom artists")
migrations_bottom = global_first_occurrence.alias("a").join(
    bottom_songs.alias("b"),
    (col("a.track_id") == col("b.track_id")) &  # Match title
    (col("b.dates") >= col("a.first_date")) &  # Same or later date
    (col("a.first_region") != col("b.region")),  # Different region
    "inner"
).select(
    col("a.track_id"),
    col("a.first_region").alias("from_region"),
    col("b.region").alias("to_region"),
    col("a.first_date").alias("from_date"),
    col("b.dates").alias("to_date")
)

migrations_bottom = migrations_bottom.dropDuplicates(['track_id', 'to_region'])

print("Get difference between songs first release and when it is popular")
migrations_pattern_top = migrations_top.withColumn("days_to_popular", datediff(col("to_date"), col("from_date")))
migrations_pattern_middle = migrations_middle.withColumn("days_to_popular", datediff(col("to_date"), col("from_date")))
migrations_pattern_bottom = migrations_bottom.withColumn("days_to_popular", datediff(col("to_date"), col("from_date")))

print("Find the global average, median, minimum and maximum: TOP")
question4_top = (
    migrations_pattern_top.groupBy(col('to_region'))
    .agg(
        avg(col('days_to_popular')).alias('avg_days_until_chart'),
        median(col('days_to_popular')).alias('median_days_until_chart'),
        min_df(col('days_to_popular')).alias('min_days_until_chart'),
        max_df(col('days_to_popular')).alias('max_days_until_chart'),
    )
    .orderBy('avg_days_until_chart', ascending=False)
)

print("Find the global average, median, minimum and maximum: MIDDLE")
question4_middle = (
    migrations_pattern_middle.groupBy(col('to_region'))
    .agg(
        avg(col('days_to_popular')).alias('avg_days_until_chart'),
        median(col('days_to_popular')).alias('median_days_until_chart'),
        min_df(col('days_to_popular')).alias('min_days_until_chart'),
        max_df(col('days_to_popular')).alias('max_days_until_chart'),
    )
    .orderBy('avg_days_until_chart', ascending=False)
)

print("Find the global average, median, minimum and maximum: BOTTOM")
question4_bottom = (
    migrations_pattern_bottom.groupBy(col('to_region'))
    .agg(
        avg(col('days_to_popular')).alias('avg_days_until_chart'),
        median(col('days_to_popular')).alias('median_days_until_chart'),
        min_df(col('days_to_popular')).alias('min_days_until_chart'),
        max_df(col('days_to_popular')).alias('max_days_until_chart'),
    )
    .orderBy('avg_days_until_chart', ascending=False)
)

print("Getting migration patterns from France")
result_df = migrations_pattern_top.select(col('to_region'), col('days_to_popular')).where(col('from_region') == 'France')
result_df = result_df.groupBy(col('to_region')).agg(avg(col('days_to_popular')).alias('avg_days_to_popular'))
result_df = result_df.orderBy(['avg_days_to_popular'], ascending=True)
result_df.write.mode('overwrite').csv('france_top.csv', header=True)

result_df = migrations_pattern_middle.select(col('to_region'), col('days_to_popular')).where(col('from_region') == 'France')
result_df = result_df.groupBy(col('to_region')).agg(avg(col('days_to_popular')).alias('avg_days_to_popular'))
result_df = result_df.orderBy(['avg_days_to_popular'], ascending=True)
result_df.write.mode('overwrite').csv('france_middle.csv', header=True)

result_df = migrations_pattern_bottom.select(col('to_region'), col('days_to_popular')).where(col('from_region') == 'France')
result_df = result_df.groupBy(col('to_region')).agg(avg(col('days_to_popular')).alias('avg_days_to_popular'))
result_df = result_df.orderBy(['avg_days_to_popular'], ascending=True)
result_df.write.mode('overwrite').csv('france_bottom.csv', header=True)

print("Getting migration patterns from Japan")
result_df = migrations_pattern_top.select(col('to_region'), col('days_to_popular')).where(col('from_region') == 'Japan')
result_df = result_df.groupBy(col('to_region')).agg(avg(col('days_to_popular')).alias('avg_days_to_popular'))
result_df = result_df.orderBy(['avg_days_to_popular'], ascending=True)
result_df.write.mode('overwrite').csv('Japan_top.csv', header=True)

result_df = migrations_pattern_middle.select(col('to_region'), col('days_to_popular')).where(col('from_region') == 'Japan')
result_df = result_df.groupBy(col('to_region')).agg(avg(col('days_to_popular')).alias('avg_days_to_popular'))
result_df = result_df.orderBy(['avg_days_to_popular'], ascending=True)
result_df.write.mode('overwrite').csv('Japan_middle.csv', header=True)

result_df = migrations_pattern_bottom.select(col('to_region'), col('days_to_popular')).where(col('from_region') == 'Japan')
result_df = result_df.groupBy(col('to_region')).agg(avg(col('days_to_popular')).alias('avg_days_to_popular'))
result_df = result_df.orderBy(['avg_days_to_popular'], ascending=True)
result_df.write.mode('overwrite').csv('Japan_bottom.csv', header=True)

print("Getting migration patterns from United Kingdom")
result_df = migrations_pattern_top.select(col('to_region'), col('days_to_popular')).where(col('from_region') == 'United Kingdom')
result_df = result_df.groupBy(col('to_region')).agg(avg(col('days_to_popular')).alias('avg_days_to_popular'))
result_df = result_df.orderBy(['avg_days_to_popular'], ascending=True)
result_df.write.mode('overwrite').csv('UK_top.csv', header=True)

result_df = migrations_pattern_middle.select(col('to_region'), col('days_to_popular')).where(col('from_region') == 'United Kingdom')
result_df = result_df.groupBy(col('to_region')).agg(avg(col('days_to_popular')).alias('avg_days_to_popular'))
result_df = result_df.orderBy(['avg_days_to_popular'], ascending=True)
result_df.write.mode('overwrite').csv('UK_middle.csv', header=True)

result_df = migrations_pattern_bottom.select(col('to_region'), col('days_to_popular')).where(col('from_region') == 'United Kingdom')
result_df = result_df.groupBy(col('to_region')).agg(avg(col('days_to_popular')).alias('avg_days_to_popular'))
result_df = result_df.orderBy(['avg_days_to_popular'], ascending=True)
result_df.write.mode('overwrite').csv('UK_bottom.csv', header=True)

print("Save to file")
question4_top.write.mode('overwrite').csv('artist_popularity_top.csv', header=True)
question4_middle.write.mode('overwrite').csv('artist_popularity_middle.csv', header=True)
question4_bottom.write.mode('overwrite').csv('artist_popularity_bottom.csv', header=True)
