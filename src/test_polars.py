import polars as pl

from settings import *

from datetime import date


# Function to run the simulation or processing, accepts the number of rows and a streaming flag
def run(num_rows, streaming=False, new_streaming=False):
    # Define product categories, subcategories, and items with frequencies.
    # If 'streaming' or 'new_streaming' is True, use `scan_csv` for streaming reading, otherwise use `read_csv`
    if streaming or new_streaming:
        products_df = pl.scan_csv(product_prices_path)
    else:
        products_df = pl.read_csv(product_prices_path)

    # Calculate the cumulative frequency (lower and upper bounds) for products based on their 'frequency'
    # This is used to create a range of frequencies for filtering products later
    products_df = products_df.with_columns([
        pl.col("frequency").cum_sum().shift(1, fill_value=0).alias("lower_frequency"),
        pl.col("frequency").cum_sum().alias("upper_frequency")
    ])

    # Initialize an empty DataFrame for purchases, which will be populated based on client data
    purchases_df = pl.DataFrame()

    # If streaming or new_streaming is enabled, convert purchases_df to a lazy DataFrame for deferred execution
    if streaming or new_streaming:
        purchases_df = purchases_df.lazy()

    # Add columns to purchases_df representing clients, dates, and weights for each purchase
    purchases_df = purchases_df.with_columns([
        pl.int_range(1, num_clients)
          .sample(num_rows, with_replacement=True)
          .alias("client"),

        pl.date_range(date(2024, 1, 1), date(2024, 12, 31))
          .sample(num_rows, with_replacement=True)
          .alias("date"),

        # Generate random weights for purchases
        (pl.int_range(num_rows) / num_rows).sample(num_rows, with_replacement=True)
        .alias("weight")
    ])

    # Filter the records based on the frequency range (lower and upper) using a conditional join
    sample_df = (
        purchases_df.join_where(
            products_df,  # Join the products data with the purchases data
            # Condition on the weight between frequencies
            pl.col("weight").is_between(pl.col("lower_frequency"), pl.col("upper_frequency"))
        )
        .select(["client", "category", "subcategory", "product", "price", "date"])
    )

    # If streaming or new_streaming is enabled, collect the data lazily. Otherwise, process it normally.
    if streaming or new_streaming:
        sample_df = sample_df.collect(streaming=streaming, new_streaming=new_streaming)

    # Save the resulting DataFrame as a Parquet file for efficient storage and future use
    sample_df.write_parquet(f"{sample_data_path}_polars")
