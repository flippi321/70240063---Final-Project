import os
import subprocess
from utils.data_generation import generate_data
from utils.data_partitioning import partition_all

should_compose = False
should_generate = False
should_partition = False

if(should_compose):
    # Compose the docker containers
    try:
        print("Composing the docker containers...")
        subprocess.run(["docker-compose", "up", "-d"], check=True)
        print("Docker containers composed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error during docker compose: {e}")

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

if should_partition:
    # We partition the data
    print("Partitioning data...")
    partitioned = partition_all(
        input_dir="data/database/dat_files", 
        output_dir="data/database/partitioned")
else:
    partitioned = True

# Run mongoimport commands after partitioning the data
if partitioned:
    print("Importing data into MongoDB...")
    uploaded_data = True
else:
    print("Data partitioning failed")


if (generated and partitioned and uploaded_data):
    print("Data generation and partitioning completed successfully")
else:
    print("Data generation failed" if not generated else "Data partitioning failed")
