import pandas as pd
import logging
import os
from sqlalchemy import create_engine

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

# Database connection setup using SQLAlchemy
db_url = "postgresql://postgres:Randika@localhost:5432/clinical_data"
try:
    engine = create_engine(db_url)
    logging.info("Database connection established successfully.")
except Exception as e:
    logging.error(f"Database connection error: {e}")
    raise

# Load data into PostgreSQL
def load_data_to_patient_demographics():
    try:
        final_data[['patient_id', 'age', 'age_group', 'gender', 'patient_demographics_other_fields']] \
            .drop_duplicates() \
            .to_sql('patient_demographics', engine, if_exists='append', index=False)
        logging.info(f"Data inserted successfully into patient_demographics")
    except Exception as e:
        logging.error(f"Error inserting data into patient_demographics: {e}")
    
def load_data_to_patient_visits():
    try:
        final_data[['patient_id', 'visit_id', 'visit_date', 'diagnosis', 'medication', 'patients_visits_other_fields']] \
            .to_sql('patient_visits', engine, if_exists='append', index=False)
        logging.info(f"Data inserted successfully into patient_visits")
    except Exception as e:
        logging.error(f"Error inserting data into patient_visits: {e}")
        
def load_data_to_patient_lab_results():
    try:
        final_data[['patient_id', 'visit_id', 'lab_test_id', 'test_date', 'test_name', 'result_value', 
                    'result_unit', 'reference_range', 'patient_lab_results_notes']] \
            .dropna() \
            .to_sql('patient_lab_results', engine, if_exists='append', index=False)
        logging.info(f"Data inserted successfully into patient_lab_results")
    except Exception as e:
        logging.error(f"Error inserting data into patient_lab_results: {e}")

def load_data_to_patient_medications():
    try:
        final_data[['patient_id', 'medication_id', 'visit_id', 'medication', 'dosage', 'start_date', 
                    'end_date', 'patient_medications_notes']] \
            .dropna() \
            .to_sql('patient_medications', engine, if_exists='append', index=False)
        logging.info(f"Data inserted successfully into patient_medications")
    except Exception as e:
        logging.error(f"Error inserting data into patient_medications: {e}")

def load_data_to_physician_assignments():
    try:
        final_data[['patient_id', 'visit_id', 'physician_id', 'physician_name', 'assignment_date', 'department']] \
            .to_sql('physician_assignments', engine, if_exists='append', index=False)
        logging.info(f"Data inserted successfully into physician_assignments")
    except Exception as e:
        logging.error(f"Error inserting data into physician_assignments: {e}")

if __name__ == "__main__":
    load_data_to_patient_demographics()
    load_data_to_patient_visits()
    load_data_to_patient_lab_results()
    load_data_to_patient_medications()
    load_data_to_physician_assignments()
