# Spark Data Analysis Pipeline

This repository contains a PySpark-based data analysis pipeline for processing a dataset of music chart information. The pipeline includes filtering, grouping, and aggregating data, as well as calculating metrics like the average, median, minimum, and maximum values for specific questions.

## Features

1. **Dataset Loading**: Load data from a CSV file.
2. **Filtering**: Remove records based on conditions such as chart type and release date.
3. **Column Selection and Transformation**: Select relevant columns and calculate new metrics.
4. **Grouping and Aggregation**: Group data by specific attributes and compute statistical measures.
5. **Custom UDF**: Calculate the maximum number of sequential days within a defined range using a custom PySpark User-Defined Function (UDF).
6. **Result Export**: Save the computed metrics as CSV files.

## Requirements

- Python 3.7+
- PySpark 3.0+
- `tqdm` for progress visualization

## Installation

### 1. Prepare the Dataset

Follow these steps to set up the dataset on the server:

1. SSH into the server:
   ssh [INSERT S NUMBER]@spark-head[INSERT CLUSTER NUMBER].eemcs.utwente.nl
2. Create a project directory:
   mkdir project
   cd project
3. Install the Kaggle CLI:
   pip install kaggle
4. Download the Spotify Charts dataset:
   kaggle datasets download -d sunnykakar/spotify-charts-all-audio-data
5. Extract the dataset:
   unzip spotify-charts-all-audio-data.zip

### 2. Install Python Packages

Install the required Python packages:
   pip install pyspark tqdm

## Usage

1. **Prepare the Input Data**: Ensure the dataset is extracted and located at `/user/[INSERT S NUMBER]/project/merged_data.csv`.
2. **Run the Script**:
   python analysis_pipeline.py
3. **View Output**: The processed data will be saved as `question1.csv` and `question2.csv` in the working directory.

## Pipeline Steps

### 1. Data Loading
- Load the input CSV file using PySpark.
- Print the dataset size after loading.

### 2. Filtering
- Remove records from the "viral50" chart.
- Exclude records with a release date earlier than 2017.

### 3. Data Transformation
- Select required columns: `track_id`, `region`, `release_date`, and `date`.
- Group by `track_id` and `region` to aggregate dates and calculate the `first_chart_day`.

### 4. Metric Calculations
- Calculate `days_until_chart` as the difference between the release date and the first chart day.
- Use a custom UDF to compute the maximum number of sequential days in the chart within a 3-day range.

### 5. Aggregation
- Compute the average, median, minimum, and maximum values for `days_until_chart` and `days_in_chart`.

### 6. Export Results
- Save the results for each analysis as CSV files.

## File Structure

- `analysis_pipeline.py`: Main script for the pipeline.
- `question1.csv`: Metrics related to `days_until_chart`.
- `question2.csv`: Metrics related to `days_in_chart`.

## Custom UDF: `max_sequential_dates_3_days_apart`

This UDF calculates the maximum number of sequential days a track remains in the chart, where each date is within 3 days of the previous one.

### Logic:
1. Sort the dates.
2. Iterate through the sorted dates, checking if the difference between consecutive dates is less than 4 days.
3. Count the length of the longest such sequence.

## Progress Tracking

The script uses `tqdm` to display progress for each processing step.



# Spark Data Analysis Pipeline

This repository contains a PySpark-based data analysis pipeline for processing a dataset of music chart information. The pipeline includes filtering, grouping, aggregating data, training machine learning models, and visualizing results.

## Features

1. **Dataset Loading**: Load data from a CSV file.
2. **Filtering and Transformation**: Remove irrelevant records and select required columns.
3. **Metric Calculations**: Calculate metrics like days in chart, highest ranking, and others.
4. **Feature Engineering**: Join audio features with metrics for modeling.
5. **Machine Learning**:
   - Train and evaluate a Random Forest Regressor.
   - Train and evaluate a Linear Regression model.
   - Analyze feature importances and coefficients.
6. **Visualization**: Generate and save feature importance plots.

## Requirements

- Python 3.7+
- PySpark 3.0+
- `tqdm` for progress visualization
- `matplotlib` for plotting
- `numpy` for numerical operations

## Installation

### 1. Prepare the Dataset

Follow these steps to set up the dataset on the server:

1. SSH into the server:
   ssh [INSERT S NUMBER]@spark-head[INSERT CLUSTER NUMBER].eemcs.utwente.nl

2. Create a project directory:
   mkdir project
   cd project

3. Install the Kaggle CLI:
   pip install kaggle

4. Download the Spotify Charts dataset:
   kaggle datasets download -d sunnykakar/spotify-charts-all-audio-data
   
6. Extract the dataset:
   unzip spotify-charts-all-audio-data.zip

### 2. Install Python Packages

Install the required Python packages:
pip install pyspark tqdm matplotlib numpy

## Usage

1. **Prepare the Input Data**: Ensure the dataset is extracted and located at `/user/s2396041/project/merged_data.csv`.
2. **Run the Script**:
   python main.py
   python prediction_model.py
4. **View Output**:
   - Machine learning metrics and coefficients will be printed to the console.
   - Feature importance plot will be saved as `rf_feature_importances.png`.
   - Processed data will be saved in the working directory.

## Pipeline Steps

1. **Data Loading and Filtering**:
   - Load the input CSV file.
   - Filter out irrelevant records, such as "viral50" charts and pre-2017 data.

2. **Metric Calculations**:
   - Calculate metrics like `days_in_chart` and `highest_ranking`.

3. **Feature Engineering**:
   - Join audio features with calculated metrics to create a modeling dataset.

4. **Random Forest Model**:
   - Train a Random Forest Regressor using audio features.
   - Evaluate model performance (RMSE, MAE, R²).
   - Save and visualize feature importances.

5. **Linear Regression Model**:
   - Train a Linear Regression model using audio features.
   - Evaluate model performance and print the regression formula.
   - Analyze coefficients and statistical significance.

## Outputs

1. **Feature Importance Plot**:
   - A bar plot of Random Forest feature importances is saved as `rf_feature_importances.png`.

2. **Model Metrics**:
   - RMSE, MAE, and R² values for both Random Forest and Linear Regression models are printed.

3. **Regression Analysis**:
   - Regression formula and coefficient statistics for the Linear Regression model.

## File Structure

- `main.py`: Main script for retrieving insights.
- `prediction_model.py`: Script training and validating a prediction model.
- `rf_feature_importances.png`: Feature importance plot.

