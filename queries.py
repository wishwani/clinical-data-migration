import pandas as pd
from sqlalchemy import create_engine
import logging


# PostgreSQL connection
POSTGRES_DB_URL = "postgresql://postgres:Randika@localhost:5432/clinical_data"  

# Connect to PostgreSQL (source database)
def connect_postgres():
    try:
        conn = create_engine(POSTGRES_DB_URL)
        logging.info("Successfully connected to PostgreSQL.")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        raise

# Execute SQL query and return result as pandas DataFrame
def execute_query(query, output_filename=None):
    try:
        conn = connect_postgres()
        df = pd.read_sql(query, conn)
        if output_filename:
            df.to_csv(output_filename, index=False)
            logging.info(f"Query result saved to {output_filename}.")
        return df
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        raise

# Retrieve all visits for a given patient 
patient_id = 'P002'
query = f"""
SELECT * 
FROM patient_visits
WHERE patient_id = '{patient_id}';
"""
patient_visits_df = execute_query(query, "outputs/patient_visits_P002.csv")
print(patient_visits_df)

# Filter patients by diagnosis and visit date range
diagnosis = 'Depression'
start_date = '2023-01-01'
end_date = '2023-12-31'
query = f"""
SELECT * 
FROM patient_visits
WHERE diagnosis = '{diagnosis}'
OR visit_date BETWEEN '{start_date}' AND '{end_date}';
"""
filtered_patients_df = execute_query(query, "outputs/filtered_patients_by_diagnosis.csv")
print(filtered_patients_df)

# Aggregate number of visits per month
query = """
SELECT EXTRACT(MONTH FROM visit_date) AS visit_month, COUNT(*) AS number_of_visits
FROM patient_visits
GROUP BY EXTRACT(MONTH FROM visit_date)
ORDER BY visit_month;
"""
visits_per_month_df = execute_query(query, "outputs/visits_per_month.csv")
print(visits_per_month_df)

# Average visits per patient
query = """
SELECT patient_id, 
       COUNT(*) AS number_of_visits,
       AVG(COUNT(*)) OVER () AS avg_visits_per_patient
FROM patient_visits
GROUP BY patient_id
HAVING COUNT(*) >= 1  
ORDER BY patient_id;
"""

avg_visits_per_patient_df = execute_query(query, "outputs/avg_visits_per_patient.csv")
print(avg_visits_per_patient_df)
