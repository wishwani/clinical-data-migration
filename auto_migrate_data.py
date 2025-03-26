import pandas as pd
from sqlalchemy import create_engine
import schedule
import time

# PostgreSQL and Supabase connection details
POSTGRES_DB_URL = "postgresql://postgres:Randika@localhost:5432/clinical_data"  
SUPABASE_DB_URL = "postgresql://postgres:postgres@127.0.0.1:54322/postgres"  

# Connect to PostgreSQL (source database)
def connect_postgres():
    engine = create_engine(POSTGRES_DB_URL)
    return engine

# Connect to Supabase (destination database)
def connect_supabase():
    engine = create_engine(SUPABASE_DB_URL)
    return engine

# Fetch data from PostgreSQL
def fetch_data_from_postgres(table_name):
    conn = connect_postgres()
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Insert data into Supabase 
def insert_data_into_supabase(df, table_name):
    engine = connect_supabase()
    df.to_sql(table_name, engine, if_exists='append', index=False) 
    print(f"Data inserted into {table_name} successfully!")

# Migration function
def migrate_data():
    tables_to_migrate = ['patient_demographics', 'patient_visits', 'patient_lab_results', 'patient_medications', 'physician_assignments']  # Add all your table names here
    
    for table in tables_to_migrate:
        print(f"Fetching data from {table}...")
        df = fetch_data_from_postgres(table)
        print(f"Inserting data into {table} in Supabase...")
        insert_data_into_supabase(df, table)

# Schedule the migration to run every day at a specific time (e.g., 02:00 AM)
def job():
    print("Starting the data migration process...")
    migrate_data()
    print("Data migration completed.")
schedule.every().day.at("02:00").do(job)

if __name__ == '__main__':
    while True:
        schedule.run_pending()
        time.sleep(60)
