import os
import re
import json
import random
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from utils.read_media import read_file_into_variable

def get_clients():
    """Connect to MongoDB for both databases (DBMS1 and DBMS2)."""
    try:
        dbms1_port = int(os.getenv("DBMS1_PORT", 27017))
        dbms2_port = int(os.getenv("DBMS2_PORT", 27018))

        client1 = MongoClient("localhost", dbms1_port)  # DBMS1 (Beijing)
        client2 = MongoClient("localhost", dbms2_port)  # DBMS2 (Hong Kong)
        return client1, client2
    except ConnectionFailure as e:
        print(f"Error connecting to MongoDB: {e}")
        exit(1)

def get_dbms_dbs():
    """Retrieve database objects for DBMS1 and DBMS2."""
    client1, client2 = get_clients()
    dbms1_db = client1["DBMS1"]  # Beijing database
    dbms2_db = client2["DBMS2"]  # Hong Kong database
    return dbms1_db, dbms2_db

def clear_database(db):
    """Clears all collections in the database."""
    try:
        for collection_name in db.list_collection_names():
            db[collection_name].delete_many({})
            print(f"Cleared collection: {collection_name} in database {db.name}")
        return True
    except Exception as e:
        print(f"Error clearing database {db.name}: {e}")
        return False
    
def clear_all_data():
    dbms1_db, dbms2_db = get_dbms_dbs()
    clear_result = clear_database(dbms1_db) and clear_database(dbms2_db)
    return clear_result

def split_query(query):
    """
    Split a query into command, collection, and JSON arguments.
    Handles spaces inside JSON structures.

    Ex.
    find Article {"id": "a1"}                           Gets split into 3 pieces
    delete Article {"id": "a1"}                         Gets split into 3 pieces
    update Article {"id": "a1"} {"title": "New Title"}  Gets split into 4 pieces
    """

    # We get the query prefix (command and collection)
    query_prefix = query.split(" ")[:2]

    # We get the arguments in json structure
    regex = "[^{]+({[^}]+})+"
    query_arguments = re.findall(regex, query)
    # print(query_arguments)

    combined_query = query_prefix + query_arguments

    print(combined_query)

    return combined_query

def print_results(collection_name, result):
    """
    Print the results of a database operation in tabular format.
    """
    print(f"\nResults from collection '{collection_name}':")
    
    # If no documents found, notify and return
    if not result:
        print("No documents found.\n")
        return

    # 1. Gather all distinct fields across the returned documents
    all_keys = set()
    for doc in result:
        all_keys |= set(doc.keys())
    # Convert to a list for indexing and consistent ordering
    all_keys = list(all_keys)

    # 2. Determine column widths (max of header or any field value)
    col_widths = []
    for key in all_keys:
        # The width for this column is the max between the length of the column name 
        # and the length of its longest value in any doc
        max_value_len = max(len(str(doc.get(key, ""))) for doc in result)
        col_widths.append(max(max_value_len, len(key)))

    # Helper function to print a row (for headers and data)
    def print_row(values):
        row_str = " | ".join(
            str(value).ljust(col_widths[i]) for i, value in enumerate(values)
        )
        print("| " + row_str + " |")

    # 3. Print header row
    header_line_len = sum(col_widths) + (3 * len(all_keys)) + 1  # for separators
    print("=" * header_line_len)
    print_row(all_keys)
    print("=" * header_line_len)

    # 4. Print each document in the result as a row
    for doc in result:
        row_values = [doc.get(k, "") for k in all_keys]
        print_row(row_values)
    print("=" * header_line_len)
    print("")  # extra spacing

def get_user_by_id(user_id):
    return {"Region": "Beijing"}

def distribute_article(dbms1_data, dbms2_data):
    # From the setup we have science/technology = 45%/55%
    # As DBMS2 already have a majority of the articles we set 80%/20% for science
    # This totals in DBMS1/DBMS2 = 36%/64%
    probabilities = [0.8, 0.2]  # 80% chance for DBMS1, 20% chance for DBMS2

    # Make a random choice
    selected_option = random.choices([dbms1_data, dbms2_data], probabilities)[0]
    return selected_option

def split_data_by_database(collection_name, data):
    """
    Splits the data based on logic specific to the collection.
    Returns two lists: data for DBMS1 and data for DBMS2.
    """
    dbms1_data = []
    dbms2_data = []

    for document in data:
        # Partition users by region
        if collection_name == "User":
            if document["region"] == "Beijing":
                dbms1_data.append(document)
            elif document["region"] == "Hong Kong":
                dbms2_data.append(document)
            else:
                print("User entry didn't contain a region, this is required")
        
        # Partition reads based on users region
        elif collection_name == "Read":
            user_id = document["uid"]
            user = get_user_by_id(user_id)
            user_region = user["region"]
            if user_region == "Beijing":
                dbms1_data.append(document)
            elif user_region == "Hong Kong":
                dbms2_data.append(document)
            else:
                print(f"Could not find user {user_id}")

        # Partition articles by category
        elif collection_name == "Article":
            if document["category"] == "science":
                selected_option = distribute_article(dbms1_data, dbms2_data)
                selected_option.append(document)
            elif document["category"] == "technology":
                dbms2_data.append(document)

        # Parition Be-Read
        elif collection_name == "Be-Read":
            dbms1_db, dbms2_db = get_dbms_dbs()
            aid = document["aid"]
            filter = eval('{"aid": ' + f'"{aid}"' + '}')
            dbms1_result = list(dbms1_db["Article"].find(filter))
            dbms2_result = list(dbms2_db["Article"].find(filter))
            combined_result = dbms1_result + dbms2_result
            matching_document = combined_result[0]

            if matching_document:
                if matching_document["category"] == "science":
                    dbms1_data.append(document)
                elif matching_document["category"] == "technology":
                    dbms2_data.append(document)
            else:
                print(f"There is no document with aid: {aid}")

        # If the user adds a non-existen collection
        else:
            print(f"Unknown collection '{collection_name}', adding to DBMS1 by default.")
            dbms1_data.append(document)

    return dbms1_data, dbms2_data

# --------------- CRUD Operations --------------- 

def handle_insert(dbms1_db, dbms2_db, collection_name, entries, should_print=True, multiple=False):
    """Insert a documents into a collection."""
    if not collection_name or not entries:
        print("Error: Insert command requires a collection name and documents.")
        return False
    
    try:
        # Convert the JSON string into a Python dictionary
        if isinstance(entries, str):
            entries = json.loads(entries)

        if not multiple:
            entries = [entries]

        # Split the data effectively between the two databases
        dbms1_data, dbms2_data = split_data_by_database(collection_name, entries)

        # Insert into dbms1
        if dbms1_data:
            dbms1_db[collection_name].insert_many(dbms1_data)
            if should_print:
                print(f"Inserted {len(dbms1_data)} documents into DBMS1, collection '{collection_name}'.")

        # Insert into dbms2
        if dbms2_data:
            dbms2_db[collection_name].insert_many(dbms2_data)
            if should_print:
                print(f"Inserted {len(dbms2_data)} documents into DBMS2, collection '{collection_name}'.")

        return True
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return False
    except Exception as e:
        print(f"Error during insert: {e}")
        return False

def handle_find(dbms1_db, dbms2_db, collection_name, filter):
    """Find documents in a collection."""
    if collection_name == None or filter == None:
        print("Error: Find command requires a collection name and a filter.")
        return
    filter_query = eval(filter)
    dbms1_result = list(dbms1_db[collection_name].find(filter_query))
    dbms2_result = list(dbms2_db[collection_name].find(filter_query))
    combined_result = dbms1_result + dbms2_result
    print_results(collection_name, combined_result)

def handle_update(dbms1_db, dbms2_db, collection_name, filter_str, update_str):
    if collection_name == None or filter_str == None or update_str == None:
        print("Error: Update command requires a collection name, a filter, and an update.")
        return
    
    filter_query = eval(filter_str)
    update_query = eval(update_str)

    # Wrap the update query with $set
    update_query = {"$set": update_query}
            
    # Attempt to update in DBMS1
    dbms1_result = dbms1_db[collection_name].update_one(filter_query, update_query)
    if dbms1_result.modified_count > 0:
        print(f"Modified {dbms1_result.modified_count} document(s) in DBMS1 collection '{collection_name}'.")
    else:
        # If no documents were updated in DBMS1, try updating in DBMS2
        dbms2_result = dbms2_db[collection_name].update_one(filter_query, update_query)
        if dbms2_result.modified_count > 0:
            print(f"Modified {dbms2_result.modified_count} document(s) in DBMS2 collection '{collection_name}'.")
        else:
            print("No matching documents found in either DBMS1 or DBMS2.")

def handle_delete(dbms1_db, dbms2_db, collection_name, filter_str):
    if collection_name == None or filter_str == None:
        print("Error: Delete command requires a collection name and a filter.")
        return
    filter_query = eval(filter_str)
    dbms1_result = dbms1_db[collection_name].delete_one(filter_query)
    if dbms1_result.deleted_count > 0:
        print(f"Deleted {dbms1_result.deleted_count} document(s) in DBMS1 collection '{collection_name}'.")
    else:
        # If no documents were deleted in DBMS1, try deleting in DBMS2
        dbms2_result = dbms2_db[collection_name].delete_one(filter_query)
        if dbms2_result.deleted_count > 0:
            print(f"Deleted {dbms2_result.deleted_count} document(s) in DBMS2 collection '{collection_name}'.")
        else:
            print("No matching documents found in either DBMS1 or DBMS2.")

"""
Non-implemented Handle Join

def handle_join(dbms1_db, dbms2_db, collection1_name, collection2_name, join_condition=None, filter1=None, filter2=None):
    ""
    Perform a JOIN operation between two collections in one or both databases.
    :param collection1_name: Name of the first collection.
    :param collection2_name: Name of the second collection.
    :param join_condition: A JSON string defining the field mapping between the two collections. Example: '{"id": "uid"}'
    :param filter1: JSON string filter to apply on the first collection.
    :param filter2: JSON string filter to apply on the second collection.
    ""
    try:
        # Parse the join condition and filters
        join_condition = json.loads(join_condition) if join_condition else {}
        filter1 = json.loads(filter1) if filter1 else {}
        filter2 = json.loads(filter2) if filter2 else {}

        pipeline = [
            {
            '$lookup': 
                {
                'from' : 'models',
                'localField' : 'references',
                'foreignField' : 'references',
                'as' : 'cellmodels'
                }},
            {
            '$match':
                 {
                'authors' : 'Migliore M',
                'cellmodels.celltypes' : 
                'Hippocampus CA3 pyramidal cell'
                }
            }]

        # Fetch data from both collections in both databases
        collection1_data = list(dbms1_db[collection1_name].find(filter1)) + list(dbms2_db[collection1_name].find(filter1))
        collection2_data = list(dbms1_db[collection2_name].find(filter2)) + list(dbms2_db[collection2_name].find(filter2))

        if not join_condition:
            print("No join condition provided. Performing a cartesian product.")
        
        # Perform the JOIN
        joined_results = []
        

        # Print the joined results
        print(f"JOIN Results between '{collection1_name}' and '{collection2_name}':")
        for result in joined_results:
            print(result)

    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
    except Exception as e:
        print(f"Error performing JOIN: {e}")

"""

# --------------- Handle Query ---------------

def handle_query(dbms1_db, dbms2_db, query):
    """Process user query and interact with databases."""
    try:
        # If user is asking for status, we don't need any splitting
        if query.lower() == "status":
            print("DBMS1 Collections:", dbms1_db.list_collection_names())
            print("DBMS2 Collections:", dbms2_db.list_collection_names())

        elif query.split(" ")[0].lower() == "join":
            # Expected usage (variable number of arguments):
            # join <collection1> <collection2> <match_key> [filter1_json] [filter2_json] [projection1_json] [projection2_json] [final_projection_json]

            query_parts = query.split(" ")

            if len(query_parts) < 5:
                print("Error: 'join' requires 5 parts: join <col1> <col2> <match_key> <filter1> <filter2>")
                return

            collection1 = query_parts[1]
            collection2 = query_parts[2]
            match_key   = query_parts[3]
            filter1_str = query_parts[4]
            filter2_str = query_parts[5] if len(query_parts) > 5 else None

            # If user didn't provide filter2, assume '{}'
            if not filter2_str:
                filter2_str = "{}"

            # parse filters (user can type {} or {"someKey":"someVal"})
            def parse_filter(json_str):
                try:
                    return eval(json_str)
                except Exception as e:
                    print(f"Error parsing filter: {json_str}: {e}")
                    return {}

            filter1 = parse_filter(filter1_str)
            filter2 = parse_filter(filter2_str)

            join_collections(
                dbms1_db=dbms1_db,
                dbms2_db=dbms2_db,
                collection1=collection1,
                collection2=collection2,
                match_key=match_key,
                filter1=filter1,
                filter2=filter2
            )

        else:
            # Split the query into command and arguments
            query_parts = split_query(query)  # Split into up to 3+ parts: command, collection, arguments & (optionally) data
            command = query_parts[0].lower()

            collection_name = query_parts[1]

            
            # Find documents matching filter in any of the Databases
            if command == "find":
                handle_find(dbms1_db, dbms2_db, collection_name, query_parts[2])

            # Update first document matching filter in any of the Databases
            elif command == "update":
                handle_update(dbms1_db, dbms2_db, query_parts[1], query_parts[2], query_parts[3])

            elif command == "find_articles_read":
                read_articles = join_user_article(dbms1_db, dbms2_db, eval(query_parts[1]))
                print_results('Top Articles', read_articles)
                #print(f"Results for articles that user {query_parts[1]} read: {read_articles}")

            elif command == "find_top_articles":
                top_articles = join_beread_article(dbms1_db, dbms2_db, query_parts[1])
                top_articles_media = []

                for article in top_articles:
                    article_media = {'id': article['id']}
                    # Retrieve text content
                    if article.get("text"):
                        article_media["text_content"] = read_file_into_variable(article["text"])
                    
                    # Retrieve image content
                    if article.get("image"):
                        image_filenames = article["image"].strip(',').split(',')  # Split multiple filenames
                        article_media["image_content"] = [
                            read_file_into_variable(image) for image in image_filenames
                        ]
                    
                    # Retrieve video content
                    if article.get("video"):
                        article_media["video_content"] = read_file_into_variable(article["video"])
                    
                    top_articles_media.append(article_media)
                print_results('Top Articles', top_articles)
                #print(f"Results for top 5 articles {query_parts[1]}: {top_articles}")

                return top_articles, top_articles_media

            # Delete first document matching filter in any of the Databases
            elif command == "delete":
                handle_delete(dbms1_db, dbms2_db, collection_name, query_parts[2])

            # Insert a document into a collection
            elif command == "insert":
                # TODO FILTER WHICH DBMS TO INSERT INTO
                handle_insert(dbms1_db, dbms2_db, collection_name, query_parts[2])

            elif command == "insert_multiple":
                # TODO FILTER WHICH DBMS TO INSERT INTO
                handle_insert(dbms1_db, dbms2_db, query_parts[1], query_parts[2], multiple=True)

            else:
                print("Unknown command. Available commands: Status, Find, Update, Delete, Insert.")

            """
            elif command == "join":
                # Parse the join query parts
                collection2 = query_parts[2]
                match_field = query_parts[3] if len(query_parts) > 3 else "id"
                conditions = json.loads(query_parts[4]) if len(query_parts) > 4 else None

                handle_join(dbms1_db, dbms2_db, collection_name, collection2, match_field, conditions)
            """

    except Exception as e:
        print(f"Error handling query: {e}")


###########################
########## JOINS ##########

def join_user_article(dbms1_db, dbms2_db, user_filter):
    """Joins User and Article tables based on user's read activity."""
    # Step 1: Fetch users matching the filter
    users = list(dbms1_db['User'].find(user_filter)) + list(dbms2_db['User'].find(user_filter))
    uids = [user['uid'] for user in users]
    
    if not uids:
        print("No users found matching the criteria.")
        return []
    
    # Step 2: Fetch reads by these users
    reads = list(dbms1_db['Read'].find({"uid": {"$in": uids}})) + list(dbms2_db['Read'].find({"uid": {"$in": uids}}))
    aids = [read['aid'] for read in reads]
    
    if not aids:
        print("No articles found read by the specified users.")
        return []
    
    # Step 3: Fetch articles by their IDs
    articles = list(dbms1_db['Article'].find({"aid": {"$in": aids}})) + list(dbms2_db['Article'].find({"aid": {"$in": aids}}))
    
    return articles


def join_beread_article(dbms1_db, dbms2_db, temporal_granularity="daily"):
    """Joins Be-Read and Article tables to get popular articles with details."""
    # Step 1: Fetch popular articles based on temporal granularity
    popular_rank = list(dbms1_db['Popular-Rank'].find({"temporalGranularity": temporal_granularity})) + \
                   list(dbms2_db['Popular-Rank'].find({"temporalGranularity": temporal_granularity}))
    
    if not popular_rank:
        print(f"No popular articles found for {temporal_granularity} granularity.")
        return []
    
    # Extract top article IDs
    article_aid_list = popular_rank[0].get('articleAidList', [])  # Assume articleAidList is sorted
    
    if not article_aid_list:
        print("No article IDs found in the popular rank.")
        return []
    
    # Step 2: Fetch article details by their IDs
    articles = list(dbms1_db['Article'].find({"aid": {"$in": article_aid_list}})) + \
               list(dbms2_db['Article'].find({"aid": {"$in": article_aid_list}}))
    
    return articles


def join_collections(
    dbms1_db, 
    dbms2_db, 
    collection1, 
    collection2, 
    match_key, 
    filter1=None, 
    filter2=None
):
    """
    Joins two collections based on a match_key.
    Pulls only filtered rows from each DB to reduce workload.
    Prints the joined result in a table.

    Args:
        dbms1_db, dbms2_db: MongoDB database objects (distributed DB).
        collection1 (str): Name of the first collection.
        collection2 (str): Name of the second collection.
        match_key (str): The field name to match on.
        filter1 (dict, optional): Filter for the first collection.
        filter2 (dict, optional): Filter for the second collection.

    Returns:
        list: A list of joined documents.
    """
    if filter1 is None:
        filter1 = {}
    if filter2 is None:
        filter2 = {}

    # 1. Fetch from COLLECTION1 in both DBMS
    data1_db1 = list(dbms1_db[collection1].find(filter1))
    data1_db2 = list(dbms2_db[collection1].find(filter1))
    data1 = data1_db1 + data1_db2

    if not data1:
        print(f"No documents found in '{collection1}' matching {filter1}.")
        return []

    # 2. Collect match_key values
    match_values = [doc.get(match_key) for doc in data1 if match_key in doc]
    match_values = list(set(match_values))  # avoid duplicates for the $in query
    if not match_values:
        print(f"No documents in '{collection1}' had the key '{match_key}'.")
        return []

    # 3. Build filter for COLLECTION2 to match on those values
    filter2_with_match = {**filter2, match_key: {"$in": match_values}}

    data2_db1 = list(dbms1_db[collection2].find(filter2_with_match))
    data2_db2 = list(dbms2_db[collection2].find(filter2_with_match))
    data2 = data2_db1 + data2_db2

    if not data2:
        print(f"No documents found in '{collection2}' matching {filter2_with_match}.")
        return []

    # 4. Build a dictionary for data2 keyed by match_key for faster lookups
    data2_dict = {}
    for doc2 in data2:
        key_val = doc2.get(match_key)
        if key_val not in data2_dict:
            data2_dict[key_val] = []
        data2_dict[key_val].append(doc2)

    # 5. Join data on match_key
    joined_data = []
    for doc1 in data1:
        doc1_key_value = doc1.get(match_key)
        if doc1_key_value is None:
            continue

        # Get all docs in data2 that match this key
        related_docs2 = data2_dict.get(doc1_key_value, [])
        for doc2 in related_docs2:
            merged_doc = {**doc1, **doc2}
            joined_data.append(merged_doc)

    # 6. Print results in table
    print_results(f"Join between {collection1} and {collection2}", joined_data)

    return joined_data
