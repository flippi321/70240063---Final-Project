from datetime import datetime, timedelta
import json
from pymongo import MongoClient
from utils.dbms_utils import distribute_article, handle_insert, get_dbms_dbs

def get_dbs():
    """ Get both databases. """
    return get_dbms_dbs()

def calculate_popularity_score(be_read_record):
    """Calculate a popularity score based on Be-Read metrics."""
    return (be_read_record.get("readNum", 0) * 1 +
            be_read_record.get("commentNum", 2) * 3 +
            be_read_record.get("agreeNum", 0) * 2 +
            be_read_record.get("shareNum", 0) * 4)

def populate_popular_rank(be_read_data):
    """Populate the Popular-Rank table based on Be-Read metrics using provided data."""
    
    # Define temporal ranges for granularity
    temporal_ranges = {
        "daily": timedelta(days=1),
        "weekly": timedelta(weeks=1),
        "monthly": timedelta(days=30),
        "20years": timedelta(weeks=1000)
    }
    
    # Use current timestamp
    now = datetime.now()
    dbms1_db, dbms2_db = get_dbs()

    for granularity, time_delta in temporal_ranges.items():
        # Calculate the start time for the granularity
        start_time = now - time_delta
        start_time = int(start_time.timestamp())

        # Filter Be-Read records based on timestamp
        filtered_records = [record for record in be_read_data if int(record.get("timestamp", 0)) >= start_time]
        
        if not filtered_records:
            print(f"No Be-Read records found for {granularity} granularity.")
            continue
        
        # Calculate popularity scores
        ranked_articles = []
        for record in filtered_records:
            popularity_score = calculate_popularity_score(record)
            ranked_articles.append((record["aid"], popularity_score))
        
        # Sort articles by popularity score in descending order
        ranked_articles.sort(key=lambda x: x[1], reverse=True)
        
        # Extract the top-5 articles
        top_articles = [aid for aid, _ in ranked_articles[:5]]
        
        # Create the Popular-Rank entry
        popular_rank_entry = {
            "id": f"popular-{granularity}-{now.strftime('%Y%m%d%H%M%S')}",
            "timestamp": now.isoformat(),  # Convert to ISO 8601 string
            "temporalGranularity": granularity,
            "articleAidList": top_articles
        }
        
        # Insert Popular-Rank entry into the database or handle as needed
        print(f"Inserted Popular-Rank entry for {granularity} granularity.")
        
        if(granularity == "daily"):
            dbms1_db["Popular-Rank"].insert_one(popular_rank_entry)
        else:
            dbms2_db["Popular-Rank"].insert_one(popular_rank_entry)

def populate_be_read_table(file_dir):
    """Populate the Be-Read table based on the Read table."""
    # Initialize a dictionary to store aggregated Be-Read data
    articles = []
    dbms1_db, dbms2_db = get_dbs()

    # We load all articles into local memory to save time
    article_categories = {}
    with open(f"{file_dir}/article.dat", "r") as infile:
        for line in infile:
            article = json.loads(line)
            articles.append(article)
            article_categories[article['aid']] = article.get('category', None)

    # We now read all reads and process them
    try:
        with open(f"{file_dir}/read.dat", "r") as file:
            # Each line in the file is a record
            records = [json.loads(line) for line in file]

            if not records:
                print("No records found in read.dat. Skipping.")
                return

            # Initialize partition dictionaries
            technology = {}
            science = {}

            # Process each record and aggregate the data
            for record in records:
                aid = record.get('aid')
                uid = record.get('uid')
                if not aid or not uid:
                    continue  # Skip records without aid or uid

                record_timestamp = int(record.get("timestamp", "0")[:10])  # Extract timestamp as integer

                # Initialize if not already in dictionary
                if aid not in technology and aid not in science:
                    category = article_categories.get(aid)
                    be_read_data = {
                        "aid": aid,
                        "readNum": 0,
                        "readUidList": [],
                        "commentNum": 0,
                        "commentUidList": [],
                        "agreeNum": 0,
                        "agreeUidList": [],
                        "shareNum": 0,
                        "shareUidList": [],
                        "timestamp": record_timestamp  # Initialize with the first timestamp
                    }

                    # Partition based on category
                    if category == "science":
                        science[aid] = be_read_data
                    elif category == "technology":
                        technology[aid] = be_read_data

                # Select the correct partition
                if aid in technology:
                    current_partition = technology
                else:
                    current_partition = science

                # Update metrics based on the current record
                current_partition[aid]["readNum"] += 1
                if uid not in current_partition[aid]["readUidList"]:
                    current_partition[aid]["readUidList"].append(uid)

                if record.get("commentOrNot"):
                    current_partition[aid]["commentNum"] += 1
                    if uid not in current_partition[aid]["commentUidList"]:
                        current_partition[aid]["commentUidList"].append(uid)

                if record.get("aggreeOrNot"):
                    current_partition[aid]["agreeNum"] += 1
                    if uid not in current_partition[aid]["agreeUidList"]:
                        current_partition[aid]["agreeUidList"].append(uid)

                if record.get("shareOrNot"):
                    current_partition[aid]["shareNum"] += 1
                    if uid not in current_partition[aid]["shareUidList"]:
                        current_partition[aid]["shareUidList"].append(uid)

                # Update the timestamp to the latest timestamp for this article
                if "timestamp" in record and int(record_timestamp) > current_partition[aid]["timestamp"]:
                    current_partition[aid]["timestamp"] = int(record_timestamp)

        # After processing, upload partitions to the respective databases
        print("Uploading technology articles to DBMS2...")
        dbms2_db["Be-Read"].insert_many(list(technology.values()))

        print("Distributing science articles between DBMS1 and DBMS2...")
        for article in science.values():
            selected_db = distribute_article(dbms1_db, dbms2_db)
            selected_db["Be-Read"].insert_one(article)

        print("Be-Read table populated successfully with partitions.")
        return list(technology.values()) + list(science.values())

    except FileNotFoundError:
        print(f"File {file_dir}/read.dat not found.")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from file: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
