import os
import re
import json
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

def get_clients():
    """Connect to MongoDB for both databases (DBMS1 and DBMS2)."""
    try:
        dbms1_port  = os.getenv("DBMS1_PORT", 27017)
        dbms2_port  = os.getenv("DBMS2_PORT", 27018)

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
                handle_update(dbms1_db, dbms2_db, collection_name, query_parts[2], query_parts[3])

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

            """
            elif command == "join":
                # Parse the join query parts
                collection2 = query_parts[2]
                match_field = query_parts[3] if len(query_parts) > 3 else "id"
                conditions = json.loads(query_parts[4]) if len(query_parts) > 4 else None

                handle_join(dbms1_db, dbms2_db, collection_name, collection2, match_field, conditions)
            """

            else:
                print("Unknown command. Available commands: Status, Find, Update, Delete, Insert.")

    except Exception as e:
        print(f"Error handling query: {e}")