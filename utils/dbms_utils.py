import os
import re
import json
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
    print(query_arguments)

    combined_query = query_prefix + query_arguments

    print(combined_query)

    return combined_query

def print_results(collection_name, result):
    """Print the results of a database operation."""
    print(f"Results from collection '{collection_name}':")
    for doc in result:
        print(doc)

def get_user_by_id(user_id):
    return {"Region": "Beijing"}

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
            user_id = data["uid"]
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
                dbms1_data.append(document)
            elif document["category"] == "technology":
                dbms2_data.append(document)
                
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
                print(f"Results for articles that user {query_parts[1]} read: {read_articles}")

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
                
                print(f"Results for top 5 articles {query_parts[1]}: {top_articles}")

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


def join_collections(dbms1_db, dbms2_db, collection1, collection2, match_key, projection1=None, projection2=None):
    """
    Generalized join between two collections in distributed databases.

    Args:
        dbms1_db, dbms2_db: MongoDB database objects.
        collection1, collection2: Names of the collections to join.
        match_key: The key to join on.
        projection1, projection2: Fields to include in the result from each collection.
    """
    # Fetch data from collection1
    data1 = list(dbms1_db[collection1].find({}, projection1)) + list(dbms2_db[collection1].find({}, projection1))
    match_values = [doc[match_key] for doc in data1]
    
    if not match_values:
        print(f"No matching documents found in {collection1}.")
        return []
    
    # Fetch matching data from collection2
    data2 = list(dbms1_db[collection2].find({match_key: {"$in": match_values}}, projection2)) + \
            list(dbms2_db[collection2].find({match_key: {"$in": match_values}}, projection2))
    
    # Combine the data
    joined_data = [{**doc1, **next((doc2 for doc2 in data2 if doc2[match_key] == doc1[match_key]), {})} for doc1 in data1]
    
    return joined_data
