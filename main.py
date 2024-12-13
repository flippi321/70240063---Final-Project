import os
import subprocess
from utils.data_generation import generate_data
from utils.data_partitioning import partition_all
from utils.hdfs_helper import process_and_upload_articles, get_hdfs_partition_path, add_hdfs_paths_to_articles
try:
    from pymongo import MongoClient
except:
    subprocess.run("pip3 install pymongo")
    from pymongo import MongoClient
import json

should_compose = True
should_generate = True
should_partition = True
should_upload_to_hdfs = True

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

if should_upload_to_hdfs and partitioned:
    print("Uploading media files to HDFS...")
    process_and_upload_articles(
        article_dir="data/database/articles",
        science_partition_file="data/database/partitioned/article_science.json",
        technology_partition_file="data/database/partitioned/article_technology.json"
    )

# Use pymongo to import data into MongoDB after partitioning
if partitioned:
    try:
        print("Importing data into MongoDB...")

        # Connect to MongoDB for both databases (DBMS1 and DBMS2)
        client1 = MongoClient("localhost", 27017)  # DBMS1 (Beijing)
        client2 = MongoClient("localhost", 27018)  # DBMS2 (Hong Kong)
        
        dbms1_db = client1["DBMS1"]  # Beijing database
        dbms2_db = client2["DBMS2"]  # Hong Kong database

        # Function to insert data into MongoDB collections
        def insert_data(collection_name, file_path, db):
            with open(file_path, 'r') as f:
                data = json.load(f)
                db[collection_name].insert_many(data)
                print(f"Data inserted into {db.name} - {collection_name} collection")

        # Insert data into DBMS1 (Beijing database)
        insert_data("User", "data/database/partitioned/user_beijing.json", dbms1_db)
        insert_data("Article", "data/database/partitioned/article_science.json", dbms1_db)
        insert_data("Read", "data/database/partitioned/read_beijing.json", dbms1_db)

        # Insert data into DBMS2 (Hong Kong database)
        insert_data("User", "data/database/partitioned/user_hongkong.json", dbms2_db)
        insert_data("Article", "data/database/partitioned/article_technology.json", dbms2_db)
        insert_data("Read", "data/database/partitioned/read_hongkong.json", dbms2_db)

        print("Data import into MongoDB completed successfully.")

    except Exception as e:
        print(f"Error during data import: {e}")
else:
    print("Data partitioning failed")


if (generated and partitioned):
    print("Data generation and partitioning completed successfully")
else:
    print("Data generation failed" if not generated else "Data partitioning failed")
