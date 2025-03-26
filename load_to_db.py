import psycopg2
import pandas as pd
from psycopg2 import sql

# Load the cleaned and transformed data
final_data = pd.read_csv('data/cleaned_data.csv')

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname='clinical_data',
    user='postgres',
    password='Randika',
    host='localhost',
    port='5432'
)
cursor = conn.cursor()

# Insert data into the patients table
def insert_patients_data(final_data):
    insert_query = """
        INSERT INTO patient_demographics (patient_id, age, age_group, gender, patient_demographics_other_fields)
        VALUES (%s, %s, %s, %s, %s)
    """
    patient_data = final_data[['patient_id', 'age', 'age_group', 'gender', 'patient_demographics_other_fields']].drop_duplicates().values.tolist()
    cursor.executemany(insert_query, patient_data)
    print(f"Inserted {len(patient_data)} records into patient_demographics.")

# Insert data into the visits table
def insert_visits_data(final_data):
    insert_query = """
        INSERT INTO patient_visits (patient_id, visit_id, visit_date, diagnosis, medication, patients_visits_other_fields)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    visit_data = final_data[['patient_id', 'visit_id', 'visit_date', 'diagnosis', 'medication', 'patients_visits_other_fields']].values.tolist()
    print(visit_data)
    cursor.executemany(insert_query, visit_data)
    print(f"Inserted {len(visit_data)} records into patient_visits.")

# Insert data into the lab results table
def insert_lab_results_data(final_data):
    insert_query = """
        INSERT INTO patient_lab_results (patient_id, visit_id, lab_test_id, test_date, test_name, result_value, result_unit, reference_range, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    lab_results_data = final_data[['patient_id', 'visit_id', 'lab_test_id', 'test_date', 'test_name', 'result_value', 'result_unit', 'reference_range', 'patient_lab_results_notes']].dropna().values.tolist()
    print(lab_results_data)
    cursor.executemany(insert_query, lab_results_data)
    print(f"Inserted {len(lab_results_data)} records into patient_lab_results.")

# Insert data into the medications table
def insert_medications_data(final_data):
    insert_query = """
        INSERT INTO patient_medications (patient_id, medication_id, visit_id, medication, dosage, start_date, end_date, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    medications_data = final_data[['patient_id', 'medication_id', 'visit_id', 'medication', 'dosage', 'start_date', 'end_date', 'patient_medications_notes']].values.tolist()
    print(medications_data)
    cursor.executemany(insert_query, medications_data)
    print(f"Inserted {len(medications_data)} records into patient_medications.")

# Insert data into the physician assignments table
def insert_physician_assignments_data(final_data):
    insert_query = """
        INSERT INTO physician_assignments (patient_id, visit_id, physician_id, physician_name, assignment_date, department)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    physician_assignments_data = final_data[['patient_id', 'visit_id', 'physician_id', 'physician_name', 'assignment_date', 'department']] \
    .drop_duplicates(subset=['physician_id'], keep='first') \
    .values.tolist()

    print(physician_assignments_data)
    cursor.executemany(insert_query, physician_assignments_data)
    print(f"Inserted {len(physician_assignments_data)} records into physician_assignments.")

def load_data_to_postgresql():
    try:
        insert_patients_data(final_data)
        insert_visits_data(final_data)
        insert_lab_results_data(final_data)
        insert_medications_data(final_data)
        insert_physician_assignments_data(final_data)

        conn.commit()
        print("Data inserted successfully into all tables!")

    except Exception as e:
        conn.rollback() 
        print(f"Error during insertion: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    load_data_to_postgresql()
