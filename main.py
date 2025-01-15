from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, array_min, collect_list, first, avg, desc, median, udf
from pyspark.sql.functions import max as max_df
from pyspark.sql.functions import min as min_df
from pyspark.sql.types import IntegerType
from datetime import datetime
from tqdm import tqdm

# Initialize Spark
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')


# Function to calculate dataset size and return a formatted message
def calculate_dataset_size(df, step_description):
    size = df.count()
    tqdm.write(f'[SUCCESS] {step_description}: Dataset size is {size}')


# UDF to calculate the max sequential dates 3 days apart
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


# Register UDF
max_sequential_udf = udf(max_sequential_dates_3_days_apart, IntegerType())

# Initialize progress bar
steps = [
    'Loading CSV file',
    'Calculating dataset size (initial)',
    'Removing viral50 records',
    'Calculating dataset size (filtered)',
    'Removing records released before 2017',
    'Calculating dataset size (filtered)',
    'Selecting required columns',
    'Grouping by track_id and region',
    'Calculating first_chart_day column',
    'Calculating days_until_chart column',
    'Dataset after filtering records with null release_date',
    'Dataset after filtering records with release_date before first_chart_day',
    'Dataset after filtering records with release_date as "0000"',
    'Finding the global average, median, min, and max for question1',
    'Saving question1.csv',
    'Calculating maximum number of sequential days in charts',
    'Finding the global average, median, min, and max for question2',
    'Saving question2.csv',

]
progress_bar = tqdm(total=len(steps), desc='Processing Steps', unit='step')
step = 0

# Step: Load CSV file
csv_path = '/user/s2396041/project/merged_data.csv'
progress_bar.set_description(steps[step])
csv_path_df1 = spark.read.csv(csv_path, header=True, inferSchema=True)
progress_bar.update(1)
step += 1

# Step: Calculate dataset size (initial)
progress_bar.set_description(steps[step])
calculate_dataset_size(csv_path_df1, 'Initial dataset loaded')
progress_bar.update(1)
step += 1

# Step: Remove viral50 records
progress_bar.set_description(steps[step])
csv_path_df2 = csv_path_df1.filter(col('chart') == 'top200')
progress_bar.update(1)
step += 1

# Step: Calculate dataset size (filtered)
progress_bar.set_description(steps[step])
calculate_dataset_size(csv_path_df2, 'Dataset after filtering viral50 records')
progress_bar.update(1)

# Step: Remove Records released before 2017
progress_bar.set_description(steps[step])
csv_path_df2 = csv_path_df2.filter("release_date > date'2017-01-01'")
progress_bar.update(1)
step += 1 

# Step: Calculate dataset size (filtered)
progress_bar.set_description(steps[step])
calculate_dataset_size(csv_path_df2, 'Dataset after filtering records released before 2017')
progress_bar.update(1)

# Step: Select required columns
progress_bar.set_description(steps[step])
csv_path_df3 = csv_path_df2.select(col('track_id'), col('region'), col('release_date'), col('date'))
progress_bar.update(1)
step += 1

# Step: Group by track_id and region
progress_bar.set_description(steps[step])
csv_path_df4 = (
    csv_path_df3.groupBy(col('track_id'), col('region'))
    .agg(first('release_date').alias('release_date'), collect_list('date').alias('dates'))
)
calculate_dataset_size(csv_path_df4, 'Dataset after grouping by track_id and region')
progress_bar.update(1)
step += 1

# Step: Calculate first_chart_day column
progress_bar.set_description(steps[step])
question1_df1 = csv_path_df4.withColumn('first_chart_day', array_min(col('dates')))
progress_bar.update(1)
step += 1

# Step: Calculate days_until_chart column
progress_bar.set_description(steps[step])
question1_df2 = question1_df1.withColumn('days_until_chart', datediff(col('first_chart_day'), col('release_date')))
progress_bar.update(1)
step += 1

# Step: Remove records where release_date is null
progress_bar.set_description(steps[step])
question1_df2 = question1_df2.filter(
    col('release_date').isNotNull()
)
calculate_dataset_size(question1_df2, 'Dataset after filtering records with null release_date')
progress_bar.update(1)
step += 1

# Step: Remove records where release_date is before first_chart_day
progress_bar.set_description(steps[step])
question1_df2 = question1_df2.filter(
    col('release_date') <= col('first_chart_day')
)
calculate_dataset_size(question1_df2, 'Dataset after filtering records with release_date before first_chart_day')
progress_bar.update(1)
step += 1


# Step: Remove records where release_date is "0000"
progress_bar.set_description(steps[step])
question1_df2 = question1_df2.filter(
    col('release_date') != '0000'
)
calculate_dataset_size(question1_df2, 'Dataset after filtering records with release_date as "0000"')
progress_bar.update(1)
step += 1

# Step: Find the global average, median, min, and max
progress_bar.set_description(steps[step])
question1_df3 = (
    question1_df2.groupBy(col('region'))
    .agg(
            avg(col('days_until_chart')).alias('avg_days_until_chart'), 
            median(col('days_until_chart')).alias('median_days_until_chart'),
            min_df(col('days_until_chart')).alias('min_days_until_chart'),
            max_df(col('days_until_chart')).alias('max_days_until_chart'),
        )
    .orderBy('avg_days_until_chart', ascending=False)
)
progress_bar.update(1)
step += 1

# Step: Save question1.csv
progress_bar.set_description(steps[step])
question1_df3.write.mode('overwrite').csv('question1.csv', header=True)
progress_bar.update(1)
step += 1

# Step: Calculate maximum number of sequential days in charts
progress_bar.set_description(steps[step])
question2_df1 = question1_df1.withColumn(
    'days_in_chart',
    max_sequential_udf(col('dates'))
)
progress_bar.update(1)
step += 1

# Step: Find the global average, median, min, and max
progress_bar.set_description(steps[step])
question2_df2 = (
    question2_df1.groupBy(col('region'))
    .agg(
            avg(col('days_in_chart')).alias('avg_days_in_chart'), 
            median(col('days_in_chart')).alias('median_days_in_chart'),
            min_df(col('days_in_chart')).alias('min_days_in_chart'),
            max_df(col('days_in_chart')).alias('max_days_in_chart'),
        )
    .orderBy('avg_days_in_chart', ascending=False)
)
progress_bar.update(1)
step += 1

# Step: Save question1.csv
progress_bar.set_description(steps[step])
question2_df2.write.mode('overwrite').csv('question2.csv', header=True)
progress_bar.update(1)
step += 1

# Close progress bar
progress_bar.close()