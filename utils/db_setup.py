import os
import json
import subprocess
from pymongo import MongoClient
from utils.data_generation import generate_data
from utils.data_partitioning import partition_all

def is_docker_running():
    """Checks if Docker containers are running."""
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=docker-compose", "--format", "{{.Names}}"],
            stdout=subprocess.PIPE,
            text=True
        )
        """
        running_containers = result.stdout.strip().split("\n")
        if running_containers:
            print("Docker containers are already running.")
            return True
        return False
        """
    except subprocess.CalledProcessError as e:
        print(f"Error checking Docker status: {e}")
        return False

def docker_compose_up():
    """Starts Docker containers using docker-compose."""
    try:
        print("Starting Docker containers...")
        subprocess.run(["docker-compose", "up", "-d"], check=True)
        print("Docker containers started successfully.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error during Docker compose: {e}")
        return False

def connect_to_mongodb(host, port, db_name):
    """Connects to a MongoDB instance."""
    try:
        client = MongoClient(host, port)
        db = client[db_name]
        db.list_collection_names()
        print(f"Connected to MongoDB: {host}:{port}/{db_name}")
        return db
    except Exception as e:
        print(f"Error connecting to MongoDB at {host}:{port}: {e}")
        return None


def clear_database(db):
    """Clears all collections in a MongoDB database."""
    try:
        for collection_name in db.list_collection_names():
            db[collection_name].delete_many({})
            print(f"Cleared collection: {collection_name} in database {db.name}")
        return True
    except Exception as e:
        print(f"Error clearing database {db.name}: {e}")
        return False

def insert_data_into_collection(db, collection_name, file_path):
    """Inserts data from a JSON file into a MongoDB collection."""
    try:
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return False
        with open(file_path, 'r') as file:
            data = json.load(file)
            db[collection_name].insert_many(data)
            print(f"Inserted data into {db.name}.{collection_name}")
        return True
    except Exception as e:
        print(f"Error inserting data into {db.name}.{collection_name}: {e}")
        return False

def upload_data_to_mongodb(input_dir):
    """Uploads data into MongoDB instances."""
    try:
        dbms1_port  = os.getenv("DBMS1_PORT", 27017)
        dbms2_port  = os.getenv("DBMS2_PORT", 27018)
        hadoop_port = os.getenv("HADOOP_PORT", 50070)

        dbms1 = connect_to_mongodb("localhost", dbms1_port, "DBMS1")
        dbms2 = connect_to_mongodb("localhost", dbms2_port, "DBMS2")
        
        if dbms1 is None or dbms2 is None:
            return False

        
        # Clear existing data
        if not clear_database(dbms1) or not clear_database(dbms2):
            return False

        data_mappings = [
            (dbms1, "User", f"{input_dir}/user_beijing.json"),
            (dbms1, "Article", f"{input_dir}/article_science.json"),
            (dbms1, "Read", f"{input_dir}/read_beijing.json"),
            (dbms2, "User", f"{input_dir}/user_hongkong.json"),
            (dbms2, "Article", f"{input_dir}/article_technology.json"),
            (dbms2, "Read", f"{input_dir}/read_hongkong.json")
        ]

        for db, collection, file_path in data_mappings:
            if not insert_data_into_collection(db, collection, file_path):
                return False
        return True

    except Exception as e:
        print(f"Error during MongoDB data upload: {e}")
        return False

def ensure_directory_exists(directory_path):
    """Checks if a directory exists and creates it if necessary."""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path, exist_ok=True)

def is_directory_empty(directory_path):
    """Checks if a directory is empty."""
    return not os.path.exists(directory_path) or not os.listdir(directory_path)

def setup_databases(
    should_compose=True, 
    input_dir='data/raw', 
    data_output_dir='data/database/articles', 
    data_partitioned_dir='data/database/partitioned', 
    dat_files_output_dir='data/database/dat_files'
):
    """Sets up databases by orchestrating Docker, data generation, partitioning, and MongoDB upload."""
    print("Setting up databases...")

    if should_compose:
        if is_docker_running():
            print("Skipping Docker setup as containers are already running.")
            return True
        else:
            if not docker_compose_up():
                return False

    ensure_directory_exists(data_output_dir)
    if is_directory_empty(data_output_dir):
        print("Generating data...")
        if not generate_data(
            num_users=10000, 
            num_articles=10000,
            num_reads=1000000, 
            input_dir=input_dir, 
            data_output_dir=data_output_dir, 
            dat_files_output_dir=dat_files_output_dir, 
            gb_size=10
        ):
            print("Data generation failed.")
            return False
        print("Data generation completed.")

    ensure_directory_exists(data_partitioned_dir)
    if is_directory_empty(data_partitioned_dir):
        print("Partitioning data...")
        if not partition_all(
            input_dir=dat_files_output_dir, 
            output_dir=data_partitioned_dir
        ):
            print("Data partitioning failed.")
            return False
        print("Data partitioning completed.")

    print("Uploading data to MongoDB...")
    if not upload_data_to_mongodb(data_partitioned_dir):
        print("Data upload to MongoDB failed.")
        return False

    print("Database setup completed successfully.")
    return True
