from utils.db_setup import setup_databases
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

def setup():
    # Setup the databases
    return setup_databases(
        should_compose=True, 
        input_dir='data/raw', 
        data_output_dir='data/database/articles', 
        data_partitioned_dir='data/database/partitioned', 
        dat_files_output_dir='data/database/dat_files'
    )

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

def get_dbms_dbs(client1, client2):
    """Retrieve database objects for DBMS1 and DBMS2."""
    dbms1_db = client1["DBMS1"]  # Beijing database
    dbms2_db = client2["DBMS2"]  # Hong Kong database
    return dbms1_db, dbms2_db

def handle_query(dbms1_db, dbms2_db, query):
    """Process user query and interact with databases."""
    if query.lower() == "status":
        print("DBMS1 Collections:", dbms1_db.list_collection_names())
        print("DBMS2 Collections:", dbms2_db.list_collection_names())
    else:
        print("Unknown query. Try 'Status' to see collections.")

# Main loop for user interaction
def main():
    usr_inp = ''
    client1, client2 = get_clients()
    dbms1_db, dbms2_db = get_dbms_dbs(client1, client2)

    try:
        # Setup the databases
        setup_success = setup()
        if not setup_success:
            print("Database setup failed. Exiting.")
            exit(1)

        # User Input Loop
        print("------------------------------------------------")
        print("Welcome to our epic DB")
        print("Available commands: Status, Find, Update, Delete, Insert, Exit.")
        print("------------------------------------------------")

        while usr_inp.lower() != 'exit':

            usr_inp = input("\nWrite your query: ").strip()

            if usr_inp.lower() != 'exit':
                handle_query(dbms1_db, dbms2_db, usr_inp)
    finally:
        # Ensure MongoDB connections are closed
        client1.close()
        client2.close()
        print("Connections closed.")

if __name__ == "__main__":
    main()