import pandas as pd
from sqlalchemy import create_engine
import logging

# PostgreSQL and Supabase connection details
POSTGRES_DB_URL = "postgresql://postgres:Randika@localhost:5432/clinical_data"  
SUPABASE_DB_URL = "postgresql://postgres:postgres@127.0.0.1:54322/postgres" 

# Connect to PostgreSQL (source database)
def connect_postgres():
    try:
        conn = create_engine(POSTGRES_DB_URL)
        logging.info("Successfully connected to PostgreSQL.")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        raise

# Connect to Supabase (destination database)
def connect_supabase():
    try:
        engine = create_engine(SUPABASE_DB_URL)
        logging.info("Successfully connected to Supabase.")
        return engine
    except Exception as e:
        logging.error(f"Error connecting to Supabase: {e}")
        raise

# Execute queries and fetch results
def execute_query(query, connection):
    try:
        df = pd.read_sql(query, connection)
        logging.info(f"Query executed successfully: {query}")
        return df
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        raise

# Fetch data from PostgreSQL
def fetch_data_from_postgres(table_name):
    try:
        conn = connect_postgres()
        query = f"SELECT * FROM {table_name};"
        df = execute_query(query, conn)
        return df
    except Exception as e:
        logging.error(f"Error fetching data from PostgreSQL table {table_name}: {e}")
        raise

# Insert data into Supabase
def insert_data_into_supabase(df, table_name):
    try:
        engine = connect_supabase()
        df.to_sql(table_name, engine, if_exists='append', index=False) 
        logging.info(f"Data inserted into {table_name} successfully in Supabase.")
    except Exception as e:
        logging.error(f"Error inserting data into Supabase table {table_name}: {e}")
        raise

# Query to get visits per patient
def get_visits_per_patient():
    query = """
    SELECT patient_id, COUNT(*) AS number_of_visits
    FROM patient_visits
    GROUP BY patient_id
    HAVING COUNT(*) > 1;
    """
    conn = connect_postgres()
    df = execute_query(query, conn)
    conn.close()
    return df

# Average visits per patient
def get_avg_visits_per_patient():
    query = """
    SELECT patient_id, 
           COUNT(*) AS number_of_visits,
           AVG(COUNT(*)) OVER () AS avg_visits_per_patient
    FROM patient_visits
    GROUP BY patient_id
    HAVING COUNT(*) > 1
    ORDER BY patient_id;
    """
    conn = connect_postgres()
    df = execute_query(query, conn)
    return df

# Migrate data
def migrate_data():
    tables_to_migrate = ['patient_demographics', 'patient_visits', 'patient_lab_results', 'patient_medications', 'physician_assignments']  # Add all your table names here

    for table in tables_to_migrate:
        logging.info(f"Fetching data from {table}...")
        print(f"Fetching data from {table}...")
        df = fetch_data_from_postgres(table)
        print(f"Inserting data into {table} in Supabase...")
        insert_data_into_supabase(df, table)
        logging.info(f"Inserting data into {table} in Supabase...")

if __name__ == '__main__':
    try:
        print("Fetching average visits per patient...")
        avg_visits_per_patient_df = get_avg_visits_per_patient()
        print(avg_visits_per_patient_df)
        
        logging.info("Starting data migration process...")
        migrate_data()
        logging.info("Data migration completed successfully.")

    except Exception as e:
        print("Exception Occured")
        logging.error(f"Error during script execution: {e}")
