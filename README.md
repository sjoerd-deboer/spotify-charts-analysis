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

1. Clone the repository:
   ```bash
   git clone https://github.com/your-repo/spark-data-analysis.git
