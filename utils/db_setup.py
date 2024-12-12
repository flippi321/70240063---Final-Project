import os
import subprocess
from pymongo import MongoClient
from data_generation import generate_data
from data_partitioning import partition_all
import json

def docker_compose_up():
    try:
        print("Composing the docker containers...")
        subprocess.run(["docker-compose", "up", "-d"], check=True)
        print("Docker containers composed successfully.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error during docker compose: {e}")
        return False
    
def upload_data_to_mongodb(input_dir):
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
        insert_data("User", f"{input_dir}/user_beijing.json", dbms1_db)
        insert_data("Article", f"{input_dir}/article_science.json", dbms1_db)
        insert_data("Read", f"{input_dir}/read_beijing.json", dbms1_db)

        # Insert data into DBMS2 (Hong Kong database)
        insert_data("User", f"{input_dir}/user_hongkong.json", dbms2_db)
        insert_data("Article", f"{input_dir}/article_technology.json", dbms2_db)
        insert_data("Read", f"{input_dir}/read_hongkong.json", dbms2_db)
 
        return True

    except Exception as e:
        print(f"Error during data import: {e}")
        return False

def setup_databases(should_compose=True, input_dir='data/raw', data_output_dir='data/database/articles', data_partitioned_dir='data/database/partitioned', dat_files_output_dir='data/database/dat_files'):
    print("Setting up databases...")
    
    # Compose docker containers
    if(should_compose):
        composed = docker_compose_up() 
        
        if(composed):
            print("Docker containers composed successfully.")
    else:
        print("Skipped docker compose")
        composed = True

    # If articles directory already contains articles, we skip data generation
    # NOTE: Python is unable to delete non-empty fodlers, so this must be done manually
    if os.listdir(data_output_dir) == [] or not os.path.exists(data_output_dir):
        print("Generating data...")
        generated = generate_data(
            num_users=10000, 
            num_articles=10000,
            num_reads=1000000, 
            input_dir=input_dir, 
            data_output_dir=data_output_dir, 
            dat_files_output_dir='data/database/dat_files', 
            gb_size=10)
        
        if(generated):
            print("Data generation completed successfully")
    else:
        generated = True

    # If partitions directory already contains the partitioned files, we skip data partitioning
    # NOTE: Python is unable to delete non-empty fodlers, so this must be done manually
    if os.listdir(data_partitioned_dir) == [] or not os.path.exists(data_partitioned_dir):
        print("Partitioning data...")
        partitioned = partition_all(
        input_dir="data/database/dat_files", 
        output_dir=data_partitioned_dir)

        if(partitioned):
            print("Data partitioning completed successfully.")
    else:
        print("Skipped partitioning")
        partitioned = True
        
    # Use pymongo to import data into MongoDB after partitioning
    if partitioned:
        print("Importing data into MongoDB...")
        uploaded = upload_data_to_mongodb(data_partitioned_dir)

        if(uploaded):
            print("Data import into MongoDB completed successfully.")
    else:
        uploaded = False
    
    success = composed and generated and partitioned and uploaded
    if(success):
        print("Database setup completed successfully.")
    else:
        print("Database setup failed.")
    
    return success
