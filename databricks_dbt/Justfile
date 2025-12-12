# Set the shell to bash for better command compatibility
set shell := ["bash", "-cu"]

startup:
    echo "Setting up source data in 'takehome.duckdb'"
    duckdb takehome.duckdb '.read "src_data/create_src_tables.sql"'
    dbt deps

# Command to refresh the dev.duckdb database and prepare dbt environment
refresh:
    echo "Refreshing takehome.duckdb..."
    rm -f takehome.duckdb
    duckdb takehome.duckdb '.read "src_data/create_src_tables.sql"'
    echo "Cleaning and installing dbt dependencies..."
    dbt clean
    dbt deps

# Command to build a specific dbt model with its upstream dependencies
trydbt:
    dbt build --select +patients_by_age
