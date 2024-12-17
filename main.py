from utils.db_setup import setup_databases

setup = setup_databases(
    should_compose=True, 
    input_dir='data/raw', 
    data_output_dir='data/database/articles', 
    data_partitioned_dir='data/database/partitioned', 
    dat_files_output_dir='data/database/dat_files')