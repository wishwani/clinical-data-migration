import pandas as pd
import logging
import os
from sqlalchemy import Date, Float, Integer, String, create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Ensure logs directory exists
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

# Configure logging
logging.basicConfig(
    filename=os.path.join(log_dir, "load_to_postgresdb.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load the cleaned and transformed data
try:
    final_data = pd.read_csv('data/cleaned_data.csv')
    logging.info("Successfully loaded cleaned_data.csv")
except Exception as e:
    logging.error(f"Error loading cleaned_data.csv: {e}")
    raise

# Use env variables for Postgres DB connection
user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT")
db = os.getenv("POSTGRES_DB")

db_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
try:
    engine = create_engine(db_url)
    logging.info("Database connection established successfully.")
except Exception as e:
    logging.error(f"Database connection error: {e}")
    raise

# Define a function to load data into any table
def load_data_to_table(table_name, columns, dtype_mapping, primary_keys=None):
    try:
        data = final_data[columns]
        
        if primary_keys:
            data = data.dropna(subset=primary_keys)

        # Insert data into the table
        data.to_sql(table_name, engine, if_exists='replace', index=False, dtype=dtype_mapping)
        logging.info(f"Data inserted successfully into {table_name}")
    except Exception as e:
        logging.error(f"Error inserting data into {table_name}: {e}")

# Define column mappings and data types for each table
patient_demographics_columns = ['patient_id', 'age', 'age_group', 'gender', 'patient_demographics_other_fields']
patient_demographics_dtype = {
    'patient_id': String(10),
    'age': Integer(),
    'age_group': String(20),
    'gender': String(10),
    'patient_demographics_other_fields': String(255)
}

patient_visits_columns = ['patient_id', 'visit_id', 'visit_date', 'visit_frequency', 'diagnosis', 'medication', 'patients_visits_other_fields']
patient_visits_dtype = {
    'patient_id': String(10),
    'visit_id': String(50),
    'visit_date': Date(),
    'visit_frequency': Integer(),
    'diagnosis': String(255),
    'medication': String(255),
    'patients_visits_other_fields': String(255)
}

patient_lab_results_columns = ['patient_id', 'visit_id', 'lab_test_id', 'test_date', 'test_name', 'result_value', 'result_unit', 'reference_range', 'patient_lab_results_notes']
patient_lab_results_dtype = {
    'patient_id': String(10),
    'visit_id': String(50),
    'lab_test_id': String(50),
    'test_date': Date(),
    'test_name': String(255),
    'result_value': Float(),
    'result_unit': String(50),
    'reference_range': String(50),
    'patient_lab_results_notes': String(255)
}

patient_medications_columns = ['patient_id', 'medication_id', 'visit_id', 'medication', 'dosage_mg', 'start_date', 'end_date', 'patient_medications_notes']
patient_medications_dtype = {
    'patient_id': String(10),
    'medication_id': String(50),
    'visit_id': String(50),
    'medication': String(255),
    'dosage_mg': Float(),
    'start_date': Date(),
    'end_date': Date(),
    'patient_medications_notes': String(255)
}

physician_assignments_columns = ['patient_id', 'visit_id', 'physician_id', 'physician_name', 'assignment_date', 'department']
physician_assignments_dtype = {
    'patient_id': String(10),
    'visit_id': String(50),
    'physician_id': String(50),
    'physician_name': String(255),
    'assignment_date': Date(),
    'department': String(255)
}

if __name__ == "__main__":
    load_data_to_table('patient_demographics', patient_demographics_columns, patient_demographics_dtype, primary_keys=['patient_id'])
    load_data_to_table('patient_visits', patient_visits_columns, patient_visits_dtype, primary_keys=['visit_id'])
    load_data_to_table('patient_lab_results', patient_lab_results_columns, patient_lab_results_dtype, primary_keys=['lab_test_id'])
    load_data_to_table('patient_medications', patient_medications_columns, patient_medications_dtype, primary_keys=['medication_id'])
    load_data_to_table('physician_assignments', physician_assignments_columns, physician_assignments_dtype, primary_keys=['patient_id', 'visit_id', 'physician_id'])
