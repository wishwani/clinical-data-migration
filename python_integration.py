import pandas as pd
from sqlalchemy import create_engine
import logging
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib_venn import venn2


# PostgreSQL and Supabase connection details
POSTGRES_DB_URL = "postgresql://postgres:Randika@localhost:5432/clinical_data"  
SUPABASE_DB_URL = "postgresql://postgres:postgres@127.0.0.1:54322/postgres" 

logging.basicConfig(
    filename='logs/data_migration_from_postgres_to_supabase.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Connect to PostgreSQL (source database)
def connect_postgres():
    try:
        conn = create_engine(POSTGRES_DB_URL)
        logging.info("Successfully connected to PostgreSQL.")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")

# Connect to Supabase (destination database)
def connect_supabase():
    try:
        engine = create_engine(SUPABASE_DB_URL)
        logging.info("Successfully connected to Supabase.")
        return engine
    except Exception as e:
        logging.error(f"Error connecting to Supabase: {e}")

# Execute queries and fetch results
def execute_query(query, connection):
    try:
        df = pd.read_sql(query, connection)
        logging.info(f"Query executed successfully: {query}")
        return df
    except Exception as e:
        logging.error(f"Error executing query: {e}")


# Fetch data from PostgreSQL
def fetch_data_from_postgres(table_name):
    try:
        conn = connect_postgres()
        query = f"SELECT * FROM {table_name};"
        df = execute_query(query, conn)
        logging.info(f"Fetched data from PostgreSQL table {table_name}.")
        return df
    except Exception as e:
        logging.error(f"Error fetching data from PostgreSQL table {table_name}: {e}")

# Insert data into Supabase
def insert_data_into_supabase(df, table_name):
    try:
        engine = connect_supabase()
        df.to_sql(table_name, engine, if_exists='replace', index=False, schema="data_migration") 
        logging.info(f"Data inserted into {table_name} successfully in Supabase.")
    except Exception as e:
        logging.error(f"Error inserting data into Supabase table {table_name}: {e}")

# Query to get visits per patient
def get_visits_per_patient():
    query = f"""
    SELECT patient_id, COUNT(*) AS number_of_visits
    FROM patient_visits
    GROUP BY patient_id
    ORDER BY patient_id;
    """
    try:
        conn = connect_postgres()
        df = execute_query(query, conn)
        df.to_csv(f'outputs/visits_per_patient.csv', index=False)
        logging.info("Saved visits per patient data to CSV.")

        # Plot
        plt.figure(figsize=(12, 6))
        sns.barplot(x=df['patient_id'], y=df['number_of_visits'], color="skyblue")
        plt.xticks(rotation=90)
        plt.xlabel("Patient ID")
        plt.ylabel("Number of Visits")
        plt.title("Number of Visits Per Patient")
        plt.savefig('outputs/visits_per_patient.png')
        return df
    except Exception as e:
        logging.error(f"Error getting visits per patient: {e}")

# Filter patients by diagnosis
def get_patients_by_diagnoise_visit_date_range():
    diagnosis = 'DEPRESSION'
    start_date = '2023-01-01'
    end_date = '2023-12-31'
    query = f"""
    SELECT patient_id, diagnosis, visit_date
    FROM patient_visits
    WHERE diagnosis = '{diagnosis}'
    OR (visit_date BETWEEN '{start_date}' AND '{end_date}');;
    """

    try:
        conn = connect_postgres()
        df = execute_query(query, conn)
        df.to_csv(f'outputs/filtered_patients_by_diagnosis_or_visit_date_range.csv', index=False)
        logging.info("Saved patients by diagnosis to CSV.")

        # Create sets of patient IDs for each group
        df['visit_date'] = pd.to_datetime(df['visit_date'], errors='coerce')
        depression_patients = set(df[df['diagnosis'] == 'DEPRESSION']['patient_id'])
        visit_2023_patients = set(df[
        (df['visit_date'] >= '2023-01-01') & 
        (df['visit_date'] <= '2023-12-31')
        ]['patient_id'])

        # Venn diagram
        plt.figure(figsize=(8, 6))
        venn2([depression_patients, visit_2023_patients], 
        set_labels=('DEPRESSION', 'Visited in 2023'),
        set_colors=('purple', 'teal'),
        alpha=0.6)

        plt.title("Patients Diagnosed with DEPRESSION or Visited in 2023")
        plt.savefig("outputs/filtered_patients_by_diagnosis_or_visit_date_range.png")
        return df
    except Exception as e:
        logging.error(f"Error getting patients by diagnosis_or_visit_date_range: {e}")
        print(f"Error getting patients by diagnosis_or_visit_date_range: {e}")

# Aggregate number of visits per month
def get_avg_visits_per_month():
    query = f"""
    SELECT EXTRACT(MONTH FROM visit_date) AS visit_month, COUNT(*) AS number_of_visits
    FROM patient_visits
    GROUP BY EXTRACT(MONTH FROM visit_date)
    ORDER BY visit_month;
    """
    try:
        conn = connect_postgres()
        df = execute_query(query, conn)
        df.to_csv(f'outputs/visits_per_month.csv')
        logging.info("Saved visits per month data to CSV.")

        # Plot
        plt.figure(figsize=(10, 5))
        sns.lineplot(x=df['visit_month'], y=df['number_of_visits'], marker="o", color="b")
        plt.xticks(range(1, 13), ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
        plt.xlabel("Month")
        plt.ylabel("Number of Visits")
        plt.title("Number of Visits Per Month")
        plt.grid()
        plt.savefig('outputs/visits_per_month.png')
        return df
    except Exception as e:
        logging.error(f"Error getting average visits per month: {e}")

# Average visits per patient
def get_avg_visits_per_patient():
    query = f"""
    SELECT patient_id, 
       COUNT(*) AS number_of_visits,
       AVG(COUNT(*)) OVER () AS avg_visits_per_patient
    FROM patient_visits
    GROUP BY patient_id
    HAVING COUNT(*) >= 1  
    ORDER BY patient_id;
    """

    try:
        conn = connect_postgres()
        df = execute_query(query, conn)
        df.to_csv(f'outputs/avg_visits_per_patient.csv')
        logging.info("Saved average visits per patient data to CSV.")

        # Plot
        avg_visits = df['number_of_visits'].mean()
        plt.figure(figsize=(8, 5))
        sns.barplot(x='patient_id', y='number_of_visits', data=df, color="purple")
        plt.axhline(avg_visits, color='red', linestyle='--', label=f'Average Visits: {avg_visits:.2f}')
        plt.xlabel("Patient ID")
        plt.ylabel("Number of Visits")
        plt.title("Average Visits Per Patient")
        plt.xticks(rotation=45)
        plt.legend()
        plt.tight_layout()
        plt.savefig('outputs/avg_visits_per_patient.png')
        return df
    except Exception as e:
        logging.error(f"Error getting average visits per patient: {e}")

# Migrate data
def migrate_data():
    tables_to_migrate = ['patient_demographics', 'patient_visits', 'patient_lab_results', 'patient_medications', 'physician_assignments']  

    for table in tables_to_migrate:
        try:
            logging.info(f"Starting migration for {table}...")
            print(f"Fetching data from {table}...")
            df = fetch_data_from_postgres(table)
            print(f"Inserting data into {table} in Supabase...")
            insert_data_into_supabase(df, table)
            logging.info(f"Migration completed for {table}.")
        except Exception as e:
            logging.error(f"Error during migration for {table}: {e}")
        

if __name__ == '__main__':
    try:
        visits_per_patient_df = get_visits_per_patient()
        print(visits_per_patient_df)

        patients_by_diagnoise_or_visit_date_range_df = get_patients_by_diagnoise_visit_date_range()
        print(patients_by_diagnoise_or_visit_date_range_df)
        
        avg_visits_per_patient_df = get_avg_visits_per_patient()
        print(avg_visits_per_patient_df)
        
        avg_visits_per_month_df = get_avg_visits_per_month()
        print(avg_visits_per_month_df)
       
        
        print("Starting data migration process...")
        migrate_data()
        print("Data migration completed successfully.")

    except Exception as e:
        logging.error(f"Exception occurred: {e}")
        
