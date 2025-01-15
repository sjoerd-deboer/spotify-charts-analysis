from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, datediff, array_min, collect_list, first, udf, min as spark_min
)
from pyspark.sql.types import IntegerType, BooleanType, FloatType
from datetime import datetime
from tqdm import tqdm

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor, LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

import matplotlib.pyplot as plt
import numpy as np

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
        if (datetime.strptime(sorted_dates[i], '%Y-%m-%d') 
            - datetime.strptime(sorted_dates[i - 1], '%Y-%m-%d')).days < 4:
            current_streak += 1
            max_streak = max(max_streak, current_streak)
        else:
            current_streak = 1
    return max_streak

max_sequential_udf = udf(max_sequential_dates_3_days_apart, IntegerType())

steps = [
    'Loading CSV file',
    'Calculating dataset size (initial)',
    'Removing viral50 records',
    'Calculating dataset size (filtered)',
    'Removing records released before 2017',
    'Calculating dataset size (filtered)',
    'Selecting required columns',
    'Grouping by track_id and region',
    'Calculating first_chart_day',
    'Calculating days_in_chart',
    'Calculating highest ranking',
    'Joining days_in_chart with highest ranking',
    'Joining audio features',
    'Assembling features and training RandomForestRegressor',
    'Evaluating RandomForest model',
    'Plotting RandomForest feature importances',
    'LinearRegression (all features) & metrics',
    'Printing linear model formula and coefficient stats'
]
progress_bar = tqdm(total=len(steps), desc='Processing Steps', unit='step')
step = 0

# Step: Loading CSV file
progress_bar.set_description(steps[step])
csv_path = '/user/s2396041/project/merged_data.csv'
df1 = spark.read.csv(csv_path, header=True, inferSchema=True)
progress_bar.update(1)
step += 1

# Step: Calculating dataset size (initial)
progress_bar.set_description(steps[step])
calculate_dataset_size(df1, 'Initial dataset loaded')
progress_bar.update(1)
step += 1

# Step: Removing viral50 records
progress_bar.set_description(steps[step])
df2 = df1.filter(col('chart') == 'top200')
progress_bar.update(1)
step += 1

# Step: Calculating dataset size (filtered)
progress_bar.set_description(steps[step])
calculate_dataset_size(df2, 'Dataset after filtering viral50 records')
progress_bar.update(1)
step += 1

# Step: Removing records released before 2017
progress_bar.set_description(steps[step])
df2 = df2.filter("release_date > date'2017-01-01'")
progress_bar.update(1)
step += 1

# Step: Calculating dataset size (filtered)
progress_bar.set_description(steps[step])
calculate_dataset_size(df2, 'Dataset after filtering records released before 2017')
progress_bar.update(1)
step += 1

# Step: Selecting required columns
progress_bar.set_description(steps[step])
df3 = df2.select(col('track_id'), col('region'), col('release_date'), col('date'), col('rank'))
progress_bar.update(1)
step += 1

# Step: Grouping by track_id and region
progress_bar.set_description(steps[step])
df4 = (
    df3.groupBy(col('track_id'), col('region'))
    .agg(
        first('release_date').alias('release_date'),
        collect_list('date').alias('dates')
    )
)
progress_bar.update(1)
step += 1

# Step: Calculating first_chart_day
progress_bar.set_description(steps[step])
df4 = df4.withColumn('first_chart_day', array_min(col('dates')))
progress_bar.update(1)
step += 1

# Step: Calculating days_in_chart
progress_bar.set_description(steps[step])
df4 = df4.withColumn('days_in_chart', max_sequential_udf(col('dates')))
progress_bar.update(1)
step += 1

# Step: Calculating highest ranking
progress_bar.set_description(steps[step])
highest_ranking_df = (
    df3.groupBy("track_id", "region")
    .agg(spark_min(col("rank")).alias("highest_ranking"))
)
progress_bar.update(1)
step += 1

# Step: Joining days_in_chart with highest ranking
progress_bar.set_description(steps[step])
model_input_df = (
    highest_ranking_df.alias("hr")
    .join(df4.select("track_id", "region", "days_in_chart"), on=["track_id", "region"], how="inner")
)
progress_bar.update(1)
step += 1

# Step: Joining audio features
progress_bar.set_description(steps[step])
features_df = (
    df2.groupBy("track_id", "region")
    .agg(
        first("explicit").alias("explicit"),
        first("af_danceability").alias("af_danceability"),
        first("af_energy").alias("af_energy"),
        first("af_key").alias("af_key"),
        first("af_loudness").alias("af_loudness"),
        first("af_mode").alias("af_mode"),
        first("af_speechiness").alias("af_speechiness"),
        first("af_acousticness").alias("af_acousticness"),
        first("af_instrumentalness").alias("af_instrumentalness"),
        first("af_liveness").alias("af_liveness"),
        first("af_valence").alias("af_valence"),
        first("af_tempo").alias("af_tempo"),
        first("af_time_signature").alias("af_time_signature")
    )
)

model_input_df = (
    model_input_df.join(features_df, on=["track_id", "region"], how="inner")
    .withColumn("explicit", col("explicit").cast(BooleanType()))
    .withColumn("af_danceability", col("af_danceability").cast(FloatType()))
    .withColumn("af_energy", col("af_energy").cast(FloatType()))
    .withColumn("af_key", col("af_key").cast(FloatType()))
    .withColumn("af_loudness", col("af_loudness").cast(FloatType()))
    .withColumn("af_mode", col("af_mode").cast(FloatType()))
    .withColumn("af_speechiness", col("af_speechiness").cast(FloatType()))
    .withColumn("af_acousticness", col("af_acousticness").cast(FloatType()))
    .withColumn("af_instrumentalness", col("af_instrumentalness").cast(FloatType()))
    .withColumn("af_liveness", col("af_liveness").cast(FloatType()))
    .withColumn("af_valence", col("af_valence").cast(FloatType()))
    .withColumn("af_tempo", col("af_tempo").cast(FloatType()))
    .withColumn("af_time_signature", col("af_time_signature").cast(FloatType()))
)
progress_bar.update(1)
step += 1

# Step: Assembling features and training RandomForestRegressor
progress_bar.set_description(steps[step])
feature_cols = [
    "explicit",
    "af_danceability",
    "af_energy",
    "af_key",
    "af_loudness",
    "af_mode",
    "af_speechiness",
    "af_acousticness",
    "af_instrumentalness",
    "af_liveness",
    "af_valence",
    "af_tempo",
    "af_time_signature"
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
data_for_model = (
    model_input_df
    .dropna(subset=feature_cols + ["highest_ranking"])
    .withColumn("label", col("highest_ranking").cast(FloatType()))
)

assembled_df = assembler.transform(data_for_model)
train_df, test_df = assembled_df.randomSplit([0.8, 0.2], seed=42)

rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="label",
    predictionCol="prediction",
    maxDepth=10,
    numTrees=50
)

rf_model_full = rf.fit(train_df)
progress_bar.update(1)
step += 1

# Step: Evaluating RandomForest model
progress_bar.set_description(steps[step])
predictions_full = rf_model_full.transform(test_df)
evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
rmse_full = evaluator.evaluate(predictions_full)

evaluator_mae = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mae")
mae_full = evaluator_mae.evaluate(predictions_full)

evaluator_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")
r2_full = evaluator_r2.evaluate(predictions_full)

tqdm.write(f"[RandomForest ALL FEATURES] RMSE = {rmse_full:.2f}, MAE = {mae_full:.2f}, R2 = {r2_full:.3f}")

importances_full = rf_model_full.featureImportances
fi_list_full = list(zip(feature_cols, importances_full.toArray()))
fi_list_full.sort(key=lambda x: x[1], reverse=True)
progress_bar.update(1)
step += 1

# Step: Plotting RandomForest feature importances
progress_bar.set_description(steps[step])
cutoff = 0.05
features_sorted_full = [x[0] for x in fi_list_full]
scores_sorted_full = [x[1] for x in fi_list_full]

plt.figure(figsize=(8, 6))
plt.bar(range(len(scores_sorted_full)), scores_sorted_full, color='skyblue')
plt.axhline(y=cutoff, color='red', linestyle='--') 
plt.xticks(range(len(features_sorted_full)), features_sorted_full, rotation=45, ha='right')
plt.title("RandomForest Feature Importances")
plt.tight_layout()
plt.savefig("rf_feature_importances.png")
plt.close()
progress_bar.update(1)
step += 1

# Step: LinearRegression (all features) & metrics
progress_bar.set_description(steps[step])
lr_assembler_all = VectorAssembler(inputCols=feature_cols, outputCol="lr_features_all")
data_for_lr_all = (
    model_input_df
    .dropna(subset=feature_cols + ["highest_ranking"])
    .withColumn("label", col("highest_ranking").cast(FloatType()))
)

assembled_df_lr_all = lr_assembler_all.transform(data_for_lr_all)
train_lr_all, test_lr_all = assembled_df_lr_all.randomSplit([0.8, 0.2], seed=42)

lr_all = LinearRegression(
    featuresCol="lr_features_all",
    labelCol="label",
    predictionCol="lr_prediction_all",
    maxIter=50,
    regParam=0.0,
    elasticNetParam=0.0
)

lr_model_all = lr_all.fit(train_lr_all)
predictions_lr_all = lr_model_all.transform(test_lr_all)

lr_rmse_all = RegressionEvaluator(
    labelCol="label",
    predictionCol="lr_prediction_all",
    metricName="rmse"
).evaluate(predictions_lr_all)

lr_r2_all = RegressionEvaluator(
    labelCol="label",
    predictionCol="lr_prediction_all",
    metricName="r2"
).evaluate(predictions_lr_all)

tqdm.write(f"[LinearRegression ALL FEATURES] RMSE = {lr_rmse_all:.2f}, R2 = {lr_r2_all:.3f}")
progress_bar.update(1)
step += 1

# Step: Printing linear model formula and coefficient stats
progress_bar.set_description(steps[step])

lr_summary_all = lr_model_all.summary
coeffs = lr_model_all.coefficients
intercept = lr_model_all.intercept
feature_coefs = list(zip(feature_cols, coeffs))

tqdm.write("\n--- Linear Model Formula (Approx) ---")
formula_str = f"predicted_highest_rank = {intercept:.4f}"
for feat_name, c in feature_coefs:
    formula_str += f" + ({c:.4f} * {feat_name})"
formula_str += " + error_term"
tqdm.write(formula_str)

tqdm.write("\n--- Coefficient Stats (Index, Feature, Coeff, StdErr, t-value, p-value) ---")
coef_stderr = lr_summary_all.coefficientStandardErrors
coef_tvals = lr_summary_all.tValues
coef_pvals = lr_summary_all.pValues

for i, f_name in enumerate(feature_cols):
    tqdm.write(f"{i}, {f_name}, {coeffs[i]:.4f}, {coef_stderr[i]:.4f}, {coef_tvals[i]:.4f}, {coef_pvals[i]:.4f}")

progress_bar.update(1)
step += 1

progress_bar.close()
