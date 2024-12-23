from datetime import datetime, timedelta
import json
from pymongo import MongoClient
from utils.dbms_utils import handle_insert, get_dbms_dbs

def get_dbs():
    """ Get both databases. """
    return get_dbms_dbs()

def calculate_popularity_score(be_read_record):
    """Calculate a popularity score based on Be-Read metrics."""
    return (be_read_record.get("readNum", 0) * 1 +
            be_read_record.get("commentNum", 2) * 3 +
            be_read_record.get("agreeNum", 0) * 2 +
            be_read_record.get("shareNum", 0) * 4)

def populate_popular_rank():
    """Populate the Popular-Rank table based on Be-Read metrics."""
    dbms1_db, dbms2_db = get_dbs()
    temporal_ranges = {
        "daily": timedelta(days=1),
        "weekly": timedelta(weeks=1),
        "monthly": timedelta(days=30),
        "20years": timedelta(weeks=1000)
    }

    be_read_records = list(dbms1_db['Be-Read'].find())
    print(f"Number of Be-Read records in DBMS1: {len(be_read_records)}")
    if be_read_records:
        print("Sample Be-Read record:", be_read_records[0])

    be_read_records = list(dbms2_db['Be-Read'].find())
    print(f"Number of Be-Read records in DBMS2: {len(be_read_records)}")
    if be_read_records:
        print("Sample Be-Read record:", be_read_records[0])


    for granularity, time_delta in temporal_ranges.items():
        # Determine which DBMS to use
        target_db = dbms1_db if granularity == "daily" else dbms2_db
        
        # Get current time and calculate the start time for the granularity
        now = datetime.now()
        start_time = now - time_delta
        start_time = int(start_time.timestamp())
        
        # Aggregate Be-Read data for the specified time range
        be_read_records = list(target_db['Be-Read'].find({"timestamp": {"$gte": start_time}}))
        
        if not be_read_records:
            print(f"No Be-Read records found for {granularity} granularity.")
            continue
        
        # Calculate popularity scores
        ranked_articles = []
        for record in be_read_records:
            popularity_score = calculate_popularity_score(record)
            ranked_articles.append((record["aid"], popularity_score))
        
        # Sort articles by popularity score in descending order
        ranked_articles.sort(key=lambda x: x[1], reverse=True)
        
        # Extract the top-5 articles
        top_articles = [aid for aid, _ in ranked_articles[:5]]
        
        # Insert into Popular-Rank table
        popular_rank_entry = {
            "id": f"popular-{granularity}-{now.strftime('%Y%m%d%H%M%S')}",
            "timestamp": now,
            "temporalGranularity": granularity,
            "articleAidList": top_articles
        }
        target_db['Popular-Rank'].insert_one(popular_rank_entry)
        print(f"Inserted Popular-Rank entry for {granularity} granularity.")


def populate_be_read_table(file_dir):
    """Populate the Be-Read table based on the Read table."""
    print("Aggregating Read data to populate Be-Read table...")

    # Initialize a dictionary to store aggregated Be-Read data
    be_read_data = {}
    dbms1_db, dbms2_db = get_dbs()

    # Assuming the Read data is stored in a file and needs to be processed
    try:
        with open(f"{file_dir}/read.dat", "r") as file:
            # Each line in the file is a record
            records = [json.loads(line) for line in file]

            if not records:
                print("No records found in read.dat. Skipping.")
                return

            # Process each record and aggregate the data
            for record in records:
                aid = record.get('aid')
                uid = record.get('uid')
                if not aid or not uid:
                    continue  # Skip records without aid or uid

                record_timestamp = int(record.get("timestamp", "0")[:10])  # Extract timestamp as integer
                
                # Initialize if not already in dictionary
                if aid not in be_read_data:
                    be_read_data[aid] = {
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

                # Update metrics based on the current record
                be_read_data[aid]["readNum"] += 1
                if uid not in be_read_data[aid]["readUidList"]:
                    be_read_data[aid]["readUidList"].append(uid)

                if record.get("commentOrNot"):
                    be_read_data[aid]["commentNum"] += 1
                    if uid not in be_read_data[aid]["commentUidList"]:
                        be_read_data[aid]["commentUidList"].append(uid)

                if record.get("aggreeOrNot"):
                    be_read_data[aid]["agreeNum"] += 1
                    if uid not in be_read_data[aid]["agreeUidList"]:
                        be_read_data[aid]["agreeUidList"].append(uid)

                if record.get("shareOrNot"):
                    be_read_data[aid]["shareNum"] += 1
                    if uid not in be_read_data[aid]["shareUidList"]:
                        be_read_data[aid]["shareUidList"].append(uid)

                # Update the timestamp to the latest timestamp for this article
                if "timestamp" in record and int(record_timestamp) > be_read_data[aid]["timestamp"]:
                    be_read_data[aid]["timestamp"] = int(record_timestamp)

        # After processing the file data, write it to a file or insert into the DB
        # Assuming handle_insert writes the result to the database
        print("Be-Read formatted, populating Be-Read...")
        handle_insert(dbms1_db, dbms2_db, 'Be-Read', list(be_read_data.values()), multiple=True)

        print("Be-Read table populated successfully.")

    except FileNotFoundError:
        print(f"File {file_dir}/read.dat not found.")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from file: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")