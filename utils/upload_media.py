import os
from pymongo import MongoClient
import gridfs
from PIL import Image
import mimetypes

# MongoDB connection details
MONGO_URI = "mongodb://localhost:27041"
DATABASE_NAME = "UnifiedDB"
ARTICLES_DIR_PATH = "data/database/articles"  # Path containing article directories

# Helper: Connect to MongoDB and get GridFS bucket
def connect_to_db():
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    bucket = gridfs.GridFS(db)
    return db, bucket

# Upload files to GridFS and link to articles
def upload_files_to_gridfs(dir, db, bucket):
    if not os.path.exists(dir):
        print(f"Directory {dir} doesn't exist.")
        return

    fs = [f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f))]
    if not fs:
        print(f"No files found in {dir}.")
        return

    for fname in fs:
        fpath = os.path.join(dir, fname)
        with open(fpath, "rb") as file:
            if bucket.exists({"filename": fname}):
                pass #print(f"File {fname} already exists.")
            else:
                file_id = bucket.put(file, filename=fname)

# Function to upload new media
def upload_new_media(file_path):
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist.")
        return None

    # Get file name
    filename = os.path.basename(file_path)

    # Connect to GridFS
    _, bucket = connect_to_db()

    # Check if file already exists in GridFS
    if bucket.exists({"filename": filename}):
        print(f"File {filename} already exists in GridFS. Skipping upload.")
        return None

    # Upload the file to GridFS
    with open(file_path, "rb") as file:
        file_id = bucket.put(file, filename=filename)
        print(f"Uploaded {filename} to GridFS with file_id {file_id}")
        return file_id

# Process all article directories for bulk media upload
def bulk_upload_articles():
    db, bucket = connect_to_db()
    debug_counter = 0
    for article_dir in os.listdir(ARTICLES_DIR_PATH):
        article_path = os.path.join(ARTICLES_DIR_PATH, article_dir)
        if os.path.isdir(article_path):
            upload_files_to_gridfs(article_path, db, bucket)
        
        debug_counter += 1
        if debug_counter % 100 == 0:
            print(f"Uploaded media for {debug_counter}/{len(os.listdir(ARTICLES_DIR_PATH))} articles.")
    

# Main entry point
if __name__ == "__main__":
    bulk_upload_articles()
