import gridfs
from pymongo import MongoClient

# MongoDB connection details
MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "UnifiedDB"

# Connect to MongoDB and GridFS
def connect_to_gridfs():
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    bucket = gridfs.GridFS(db)
    return bucket

# Read a file into a variable
def read_file_into_variable(filename):
    bucket = connect_to_gridfs()
    
    # Check if the file exists in GridFS
    if bucket.exists({"filename": filename}):
        file = bucket.find_one({"filename": filename})
        file_data = file.read()  # Read the file's content into a variable
        return file_data
    else:
        print(f"File {filename} does not exist in GridFS.")
        return None

# How to read a file:
# test = read_file_into_variable("text_a9981.txt")
# print(test)