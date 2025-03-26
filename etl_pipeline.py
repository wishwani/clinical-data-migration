import pandas as pd
import os

# File paths
base_path = "data/"
files = {
    "patient_demographics": "patient_demographics.csv",
    "patient_visits": "patient_visits.csv",
    "patient_lab_results": "patient_lab_results.csv",
    "physician_assignments": "physician_assignments.csv",
    "patient_medications": "patient_medications.csv"
}

# Load CSV files
def load_data():
    data = {}
    for key, filename in files.items():
        file_path = os.path.join(base_path, filename)
        data[key] = pd.read_csv(file_path)
    return data

# Clean data
def clean_data(data):
    
    for key, df in data.items():
        df.drop_duplicates(inplace=True)

        if 'age' in df.columns:
            df.fillna({'age':-1}, inplace=True)

        if 'gender' in df.columns:
            df['gender'] = df['gender'].fillna('unknown')
            
        if 'end_date' in df.columns:
            df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')  
            df.fillna({'end_date':pd.Timestamp('1900-01-01').date()}, inplace=True)

        for column in df.select_dtypes(include=['object']).columns: 
             df[column] = df[column].map(lambda x: x.upper() if isinstance(x, str) else x)
        data[key] = df 
        
    return data

# Merge datasets
def merge_data(data):
    merged_data = data["patient_demographics"]
    
    merged_data = merged_data.merge(data["patient_visits"], on="patient_id", how="left")
    merged_data = merged_data.merge(data["patient_lab_results"], on=["patient_id", "visit_id"], how="left")
    merged_data = merged_data.merge(data["patient_medications"], on=["patient_id", "visit_id"], how="left")
    merged_data = merged_data.merge(data["physician_assignments"], on=["patient_id", "visit_id"], how="left")
    merged_data = merged_data.drop(columns=["medication_x"]).rename(columns={"medication_y": "medication"})
    merged_data = merged_data.rename(columns={"other_fields_x": "patient_demographics_other_fields", "other_fields_y": "patients_visits_other_fields"})
    merged_data = merged_data.rename(columns={"notes_x": "patient_lab_results_notes", "notes_y": "patient_medications_notes"})
    merged_data['age_group'] = merged_data['age'].apply(lambda x: '18-35' if x <= 35 else '36-65' if x <= 65 else '65+')

    if 'test_date' in merged_data.columns:
            merged_data.fillna({'test_date':pd.Timestamp('1900-01-01').date()}, inplace=True)
    if 'start_date' in merged_data.columns:
            merged_data.fillna({'start_date':pd.Timestamp('1900-01-01').date()}, inplace=True)
    if 'end_date' in merged_data.columns:
            merged_data.fillna({'end_date':pd.Timestamp('1900-01-01').date()}, inplace=True)
    return merged_data

# Save cleaned and merged data
def save_data(merged_data):
    output_path = os.path.join(base_path, "cleaned_data.csv")
    merged_data.to_csv(output_path, index=False)
    print(f"Cleaned data saved to {output_path}")

# ETL Pipeline
def main():
    
    print("Loading data...")
    data = load_data()
    
    print("Cleaning data...")
    cleaned_data = clean_data(data)

    print("Merging data...")

    merged_data = merge_data(cleaned_data)
    
    print("Saving cleaned data...")
    save_data(merged_data)

if __name__ == "__main__":
    main()
