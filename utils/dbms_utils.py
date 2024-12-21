import os
import re
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

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

def get_dbms_dbs(client1, client2):
    """Retrieve database objects for DBMS1 and DBMS2."""
    dbms1_db = client1["DBMS1"]  # Beijing database
    dbms2_db = client2["DBMS2"]  # Hong Kong database
    return dbms1_db, dbms2_db


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

# --------------- CRUD Operations --------------- 
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

def handle_query(dbms1_db, dbms2_db, query):
    """Process user query and interact with databases."""
    try:
        # Split the query into command and arguments
        query_parts = split_query(query)  # Split into up to 3 parts: command, collection, arguments
        command = query_parts[0].lower()

        if command == "status":
            print("DBMS1 Collections:", dbms1_db.list_collection_names())
            print("DBMS2 Collections:", dbms2_db.list_collection_names())

        # Find documents matching filter in any of the Databases
        elif command == "find":
            handle_find(dbms1_db, dbms2_db, query_parts[1], query_parts[2])

        # Update first document matching filter in any of the Databases
        elif command == "update":
            handle_update(dbms1_db, dbms2_db, query_parts[1], query_parts[2], query_parts[3])

        # Delete first document matching filter in any of the Databases
        elif command == "delete":
            handle_delete(dbms1_db, dbms2_db, query_parts[1], query_parts[2])

        # Insert a document into a collection
        elif command == "insert":
            # TODO FILTER WHICH DBMS TO INSERT INTO
            if len(query_parts) < 3:
                print("Error: Insert command requires a collection name and a document.")
                return
            collection_name = query_parts[1]
            document = eval(query_parts[2])
            result = dbms2_db[collection_name].insert_one(document)
            print(f"Inserted document with ID {result.inserted_id} into collection '{collection_name}'.")

        elif command == "shitfuck":
            print("Shitfucking...")
            for doc in dbms2_db["User"].find():
                print(doc)

        else:
            print("Unknown command. Available commands: Status, Find, Update, Delete, Insert.")

    except Exception as e:
        print(f"Error handling query: {e}")