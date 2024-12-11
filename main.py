import os
from utils.data_generation import generate_data
from utils.data_partitioning import partition_all

should_generate = False

if should_generate:
    # We generate 10GB of data
    print("Generating data...")
    generated = generate_data(
        num_users=10000, 
        num_articles=10000,
        num_reads=1000000, 
        input_dir='data/raw', 
        data_output_dir='data/database/articles', 
        dat_files_output_dir='data/database/dat_files', 
        gb_size=10)
else:
    generated = True

# We partition the data
print("Partitioning data...")
partitioned = partition_all(
    input_dir="data/database/dat_files", 
    output_dir="data/database/partitioned")


if (generated and partitioned):
    print("Data generation and partitioning completed sucessfully")
else:
    print("Data generation failed" if not generated else "Data partitioning failed")