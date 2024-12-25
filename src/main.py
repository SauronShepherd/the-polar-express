from pyspark.sql import SparkSession

import polars as pl

import timeit

from functools import partial

from settings import *
import test_polars
import test_pyspark


# Function to run a method and return its total execution time
def run(method):
    total_time = timeit.timeit(method, number=num_tests)
    return f"{total_time:.4f}s"


# Function to initialize a Spark session with specific configurations
def initialize_spark():
    print("Initializing Spark Session ...")
    # Configure and create the SparkSession with local execution settings
    (SparkSession.builder
                 .appName("spark_queries")
                 .master("local[*]")
                 .config("spark.driver.memory", "4G")
                 .getOrCreate())
    # Run the PySpark tests with a fixed number of rows (1,000 in this case)
    run(partial(test_pyspark.run, 1_000))


# Dictionary to store results for each number of rows and for each framework (Polars, Polars Streaming, PySpark)
results = {"num_rows": [], "polars": [], "polars_streaming": [],
           "polars_new_streaming": [], "pyspark": []}

# Iterate over each row size and store results for each method
for num_rows in all_num_rows:
    # Format the number of rows for display (e.g., 1,000,000 -> 1_000_000)
    num_rows_str = f"{num_rows:,}".replace(",", "_")
    results["num_rows"].append(num_rows_str)

    # Run Polars test
    print(f"Running Polars [{num_rows_str} rows - {num_tests} times] ...")
    results["polars"].append(run(partial(test_polars.run, num_rows)))

    # Run Polars Streaming test
    print(f"Running Polars Streaming [{num_rows_str} rows - {num_tests} times] ...")
    results["polars_streaming"].append(run(partial(test_polars.run, num_rows, streaming=True)))

    # Run Polars New Streaming test
    print(f"Running Polars New Streaming [{num_rows_str} rows - {num_tests} times] ...")
    results["polars_new_streaming"].append(run(partial(test_polars.run, num_rows, new_streaming=True)))

# Initialize Spark session
initialize_spark()

# Run PySpark tests for each row size and store the time
for num_rows in all_num_rows:
    num_rows_str = f"{num_rows:,}".replace(",", "_")

    # Run PySpark test
    print(f"Running Pyspark [{num_rows_str} rows - {num_tests} times] ...")
    results["pyspark"].append(run(partial(test_pyspark.run, num_rows)))

# Convert the results dictionary to a Polars DataFrame for easy viewing and display
df = pl.DataFrame(results)
print(df)
