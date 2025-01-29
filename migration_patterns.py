from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, datediff, avg, round

# Initialize SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print('Loading Spotify data...')
path = '/user/s2605945/project/merged_data.csv'
df1 = spark.read.csv(path, header=True)

print('Preprocessing data...')
df2 = df1.filter("release_date >= date'2017-01-01'")
df3 = df2.filter(col('af_danceability') >= 0.75)
df4 = df3.filter(col('chart') == 'top200')
df5 = df4.orderBy(['title', 'date', 'region'], ascending=True)
df6 = df5.select(col('title'), col('date'), col('region'))

# Step 1: Find the first occurrence of each title globally
global_first_occurrence = df6.groupBy("title").agg(
    min("date").alias("first_date")  # Calculate the first date per title
).alias("agg").join(
    df6.alias("orig"),  # Alias the original DataFrame
    (col("agg.title") == col("orig.title")) & (col("agg.first_date") == col("orig.date")),  # Disambiguate column references
    "inner"
).select(
    col("agg.title"),
    col("agg.first_date"),
    col("orig.region").alias("first_region")
)

# Step 2: Find migrations from the first occurrence
migrations = global_first_occurrence.alias("a").join(
    df6.alias("b"),
    (col("a.title") == col("b.title")) &  # Match title
    (col("b.date") >= col("a.first_date")) &  # Same or later date
    (col("a.first_region") != col("b.region")),  # Different region
    "inner"
).select(
    col("a.title"),
    col("a.first_region").alias("from_region"),
    col("b.region").alias("to_region"),
    col("a.first_date").alias("from_date"),
    col("b.date").alias("to_date")
)

migrations_pattern = migrations.withColumn("days_to_popular", datediff(col("to_date"), col("from_date")))

# countries = ['Luxembourg', 'France', 'Dominican Republic', 'Japan', 'United Kingdom']

# Luxembourg
result_df = migrations_pattern.select(col('to_region'), col('days_to_popular'),).where(col('from_region') == 'Luxembourg')
result_df = result_df.groupBy('to_region').agg(round(avg(col('days_to_popular')), 0).alias('avg_days_to_popular'))
result_df = result_df.orderBy(['avg_days_to_popular'], ascending=True)

result_df.write.mode('overwrite').csv('/user/s2605945/project/luxembourg.csv', header=True)

# France
result_df = migrations_pattern.select(col('to_region'), col('days_to_popular'),).where(col('from_region') == 'France')
result_df = result_df.groupBy('to_region').agg(round(avg(col('days_to_popular')), 0).alias('avg_days_to_popular'))
result_df = result_df.orderBy(['avg_days_to_popular'], ascending=True)

result_df.write.mode('overwrite').csv('/user/s2605945/project/france.csv', header=True)

# Dominican Republic
result_df = migrations_pattern.select(col('to_region'), col('days_to_popular'),).where(col('from_region') == 'Dominican Republic')
result_df = result_df.groupBy('to_region').agg(round(avg(col('days_to_popular')), 0).alias('avg_days_to_popular'))
result_df = result_df.orderBy(['avg_days_to_popular'], ascending=True)

result_df.write.mode('overwrite').csv('/user/s2605945/project/dominican_republic.csv', header=True)

# Japan
result_df = migrations_pattern.select(col('to_region'), col('days_to_popular'),).where(col('from_region') == 'Japan')
result_df = result_df.groupBy('to_region').agg(round(avg(col('days_to_popular')), 0).alias('avg_days_to_popular'))
result_df = result_df.orderBy(['avg_days_to_popular'], ascending=True)

result_df.write.mode('overwrite').csv('/user/s2605945/project/japan.csv', header=True)

# United Kingdom
result_df = migrations_pattern.select(col('to_region'), col('days_to_popular'),).where(col('from_region') == 'United Kingdom')
result_df = result_df.groupBy('to_region').agg(round(avg(col('days_to_popular')), 0).alias('avg_days_to_popular'))
result_df = result_df.orderBy(['avg_days_to_popular'], ascending=True)

result_df.write.mode('overwrite').csv('/user/s2605945/project/united_kingdom.csv', header=True)