import pandas as pd
import os
import logging

# Ensure logs directory exists
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

# Set up logging
logging.basicConfig(
    filename="logs/etl_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# File paths
base_path = "data/"
files = {
    "patient_demographics": "patient_demographics.csv",
    "patient_visits": "patient_visits.csv",
    "patient_lab_results": "patient_lab_results.csv",
    "physician_assignments": "physician_assignments.csv",
    "patient_medications": "patient_medications.csv"
}

# Function to modify reference range format (e.g., "5-10" to "5000-10000")
def modify_range(value):
    if isinstance(value, str) and '-' in value:
        start, end = value.split('-')
        start, end = int(start), int(end)
        return f"{start*1000}-{end*1000}"
    return value

# Load CSV files
def load_data():
    data = {}
    for key, filename in files.items():
        file_path = os.path.join(base_path, filename)
        try:
            data[key] = pd.read_csv(file_path)
            logging.info(f"Loaded {filename} successfully.")
        except FileNotFoundError:
            logging.error(f"File {filename} not found.")
        except Exception as e:
            logging.error(f"Error loading {filename}: {e}")
    return data

# Clean data
def clean_data(data):
    
    for key, df in data.items():
        try:
            # Remove duplicate rows from the data
            df.drop_duplicates(inplace=True)
            logging.info(f"Removed duplicates in {key}")

            # Assumption: If a "dosage" column exists (e.g., "5mg"), strip the unit and convert to a numeric float in mg
            if "dosage" in df.columns:
                df['dosage'] = df['dosage'].str.replace('mg', '', regex=False).astype(float)
                df.rename(columns={'dosage': 'dosage_mg'}, inplace=True) # Rename to 'dosage_mg'

            # Assumption: Fill missing age values with the median and convert to integer
            if "age" in df.columns:
                df["age"] = df["age"].fillna(df["age"].median()).astype(int)

            for index, row in df.iterrows():

                # Normalize "result_unit" to uppercase if it exists
                if "result_unit" in df.columns:
                    df["result_unit"] = df["result_unit"].str.upper()

                # Assumption: For "G/DL" units, convert result_value to "MG/DL" by multiplying by 1000; also update reference_range accordingly
                if "result_unit" in df.columns and "reference_range" in df.columns:
                    g_dl_condition = df["result_unit"] == "G/DL"
                    
                    # Update the 'result_unit' column with only mg/dl and update the'result_value' and 'reference_range' accordingly
                    df.loc[g_dl_condition, "result_value"] *= 1000
                    df.loc[g_dl_condition, "result_unit"] = "MG/DL"
                    df.loc[g_dl_condition, "reference_range"] = df.loc[g_dl_condition, "reference_range"].apply(modify_range)
                
                # For patient_lab_results, set notes to "NORMAL", "LOW", or "HIGH" based on reference_range comparison
                if key == "patient_lab_results" and pd.notnull(row["result_value"]) and "reference_range":
                    if pd.isnull(row["notes"]):
                        min_range, max_range = map(int, row["reference_range"].split("-"))
                        if min_range <= row["result_value"] <= max_range:
                            df.at[index, "notes"] = "NORMAL"
                        elif row["result_value"] < min_range:
                            df.at[index, "notes"] = "LOW"
                        elif row["result_value"] > max_range:
                           df.at[index, "notes"] = "HIGH"

            # Convert date fields to standard format
            for date_column in ['test_date', 'start_date', 'end_date', 'assignment_date']:
                if date_column in df.columns:
                    df[date_column] = pd.to_datetime(df[date_column], errors='coerce').dt.strftime('%Y-%m-%d')
            
            # Assumption: Fill missing string values with "UNKNOWN" and convert to uppercase
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].fillna("UNKNOWN").str.upper()

            # Assumption: Fill missing numeric values with placeholder -999
            for col in df.select_dtypes(include=['number']).columns:
                df[col] = df[col].fillna(-999)
       
            logging.info(f"Cleaned data for {key}")

        except Exception as e:
            logging.error(f"Error cleaning data for {key}: {e}")

        data[key] = df

    return data


# Merge datasets
def merge_data(data):
    try:
        merged_data = data["patient_demographics"]
        merged_data = merged_data.merge(data["patient_visits"], on="patient_id", how="left")
        merged_data = merged_data.merge(data["patient_lab_results"], on=["patient_id", "visit_id"], how="left")
        merged_data = merged_data.merge(data["patient_medications"], on=["patient_id", "visit_id"], how="left")
        merged_data = merged_data.merge(data["physician_assignments"], on=["patient_id", "visit_id"], how="left")

        # Resolve column names conflicts from merge:
        # Drop 'medication_x' from patient_visits and rename 'medication_y' to 'medication'
        if "medication_x" in merged_data.columns:
            merged_data = merged_data.drop(columns=["medication_x"]).rename(columns={"medication_y": "medication"})
        
        # Rename 'other_fields_x' to 'patient_demographics_other_fields'
        # Rename 'other_fields_y' to 'patients_visits_other_fields'
        merged_data = merged_data.rename(columns={"other_fields_x": "patient_demographics_other_fields", "other_fields_y": "patients_visits_other_fields"})
        
        # Rename 'notes_x' to 'patient_lab_results_notes'
        # Rename 'notes_y' to 'patient_medications_notes'
        merged_data = merged_data.rename(columns={"notes_x": "patient_lab_results_notes", "notes_y": "patient_medications_notes"})
        
        # Derive 'age_group' column from 'age' values
        merged_data['age_group'] = merged_data['age'].apply(lambda x: '18-35' if x <= 35 else '36-65' if x <= 65 else '65+')

        # Calculate visit frequency per patient
        visit_counts = data["patient_visits"].groupby("patient_id")["visit_id"].count().reset_index()
        visit_counts.rename(columns={"visit_id": "visit_frequency"}, inplace=True)

        # Derive 'visit frequency' column from calculated visit frequency 
        merged_data = merged_data.merge(visit_counts, on="patient_id", how="left")
        merged_data["visit_frequency"] = merged_data["visit_frequency"].fillna(0).astype(int)

        logging.info("Data merging successful.")

    except KeyError as e:
        logging.error(f"Missing column during merge: {e}")
    except Exception as e:
        logging.error(f"Error during merge: {e}")

    return merged_data

# Save cleaned and merged data
def save_data(merged_data):
    output_path = os.path.join(base_path, "cleaned_data.csv")
    try:
        merged_data.to_csv(output_path, index=False)
        logging.info(f"Cleaned data saved to {output_path}")
    except Exception as e:
        logging.error(f"Error saving cleaned data: {e}")


def main():

    logging.info("ETL pipeline started.")
    
    print("Loading data...")
    data = load_data()
    
    print("Cleaning data...")
    cleaned_data = clean_data(data)

    print("Merging data...")

    merged_data = merge_data(cleaned_data)
    
    print("Saving cleaned data...")
    save_data(merged_data)

    logging.info("ETL pipeline (Load, Transform) completed successfully.")

if __name__ == "__main__":
    main()