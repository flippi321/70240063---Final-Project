from utils.db_setup import setup_databases
from utils.dbms_utils import get_clients, get_dbms_dbs, split_query, handle_query

def setup():
    """Setup the databases."""
    return setup_databases(
        should_compose=True, 
        input_dir='data/raw', 
        data_output_dir='data/database/articles', 
        data_partitioned_dir='data/database/partitioned', 
        dat_files_output_dir='data/database/dat_files',
    )

# Main loop for user interaction
def main():
    usr_inp = ''
    dbms1_db, dbms2_db = get_dbms_dbs()

    try:
        # Setup the databases
        setup_success = setup()
        if not setup_success:
            print("Database setup failed. Exiting.")
            exit(1)

        # User Input Loop
        print("------------------------------------------------")
        print("Welcome to our Distributed Databse System")
        print("Available commands: status, find, find_top_articles, find_articles_read, update, delete, insert, join, exit.")
        print("------------------------------------------------")

        while usr_inp.lower() != 'exit':

            usr_inp = input("\nWrite your query: ").strip()

            if usr_inp.lower() != 'exit':
                handle_query(dbms1_db, dbms2_db, usr_inp)
    finally:
        # Ensure MongoDB connections are closed
        client1, client2 = get_clients()
        client1.close()
        client2.close()
        print("Connections closed.")

if __name__ == "__main__":
    main()