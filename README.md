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

1. **Prepare the Input Data**: Ensure the dataset is extracted and located at `/user/[INSERT S NUMBER]/project/merged_data.csv`.
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

![rf_feature_importances](https://github.com/user-attachments/assets/2baef32d-2a04-42ba-8fa9-427e074f7aae)

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

