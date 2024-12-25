from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, lag, sum, expr, from_unixtime

from settings import *

from datetime import datetime


# Function to run the PySpark simulation with the given number of rows
def run(num_rows):

    # Get the active Spark session (assumes a Spark session is already initialized)
    spark = SparkSession.getActiveSession()

    # Read product data (CSV file) into a DataFrame, assuming the file has a header
    products_df = spark.read.option("header", "true").csv(product_prices_path)

    # Calculate the cumulative frequency for each product, and determine lower and upper frequency bounds for sampling
    # Define a window spec to calculate cumulative sums and use lag function
    window_frequency = Window.partitionBy(lit(None)).orderBy("frequency")

    # Add new columns to the products DataFrame for cumulative frequency and frequency bounds (lower and upper)
    products_df = (
        products_df.withColumn("cumulative_frequency", sum("frequency")
                               .over(window_frequency))
                   .withColumn("lower_frequency", lag("cumulative_frequency", 1, 0)
                               .over(window_frequency))
                   .withColumn("upper_frequency", col("cumulative_frequency"))
    )

    # Create a DataFrame to simulate purchases (with random IDs), which will later be augmented with other data
    # Generate a range from 0 to num_rows for client IDs
    purchases_df = spark.range(0, num_rows).toDF("id")

    # Generate random client IDs between 1 and num_clients for each purchase
    purchases_df = purchases_df.withColumn("client", expr(f"rand() * {num_clients} + 1").cast("int"))

    # Generate random dates for 2024. Convert the date range into milliseconds and create a 'date' column from UNIX time
    start_of_2024 = int(datetime(2024, 1, 1, 0, 0, 0).timestamp())
    end_of_2024 = int(datetime(2024, 12, 31, 23, 59, 59).timestamp())

    # Create a random date for each purchase by generating a random number between the start and end timestamps
    purchases_df = purchases_df.withColumn(
        "date_millis",
        expr(f"cast({start_of_2024} + floor(rand() * {end_of_2024 - start_of_2024}) as bigint)")
    ).withColumn("date", from_unixtime(col("date_millis")))

    # Generate a random weight for each purchase, which will be used for frequency-based filtering
    purchases_df = purchases_df.withColumn("weight", expr("rand()"))

    # Filter records by joining the purchases with the products, based on the weight being between the product's
    # frequency bounds
    # Join the purchases_df and products_df on the condition that the 'weight' is within the frequency bounds
    sample_df = (purchases_df
                 .join(products_df,
                       col("weight").between(col("lower_frequency"), col("upper_frequency")), "left")
                 .select("client", "category", "subcategory", "product", "price", "date")
                 )

    # Save the resulting DataFrame as a Parquet file (for efficient storage and querying)
    sample_df.write.mode("overwrite").format("parquet").save(f"{sample_data_path}_pyspark")
