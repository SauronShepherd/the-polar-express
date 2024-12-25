# Number of test iterations to run for each method
num_tests = 3

# List of different row sizes to test on
all_num_rows = [1_000_000, 5_000_000, 25_000_000, 50_000_000, 75_000_000, 100_000_000]

# Number of clients to simulate (1 million clients)
num_clients = 1_000_000

# Path to the directory where data files are stored
data_path = "./data"

# Path to the product prices CSV file within the 'data' directory
product_prices_path = f"{data_path}/product_prices.csv"

# Path to the sample data directory within the 'data' directory
sample_data_path = f"{data_path}/sample_data"
