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
            df.drop_duplicates(inplace=True)
            logging.info(f"Removed duplicates in {key}")

            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].fillna("UNKNOWN").str.upper()

            if "age" in df.columns:
                df["age"] = pd.to_numeric(df["age"], errors='coerce').fillna(-1).astype(int)

            if "medication" in df.columns:
                df['medication'] = df['medication'].fillna("UNKNOWN")

            for col in df.select_dtypes(include=['number']).columns:
                df[col] = df[col].fillna(0)

            for date_column in ['test_date', 'start_date', 'end_date']:
                if date_column in df.columns:
                    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
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
    
        if "medication_x" in merged_data.columns:
            merged_data = merged_data.drop(columns=["medication_x"]).rename(columns={"medication_y": "medication"})
        merged_data = merged_data.rename(columns={"other_fields_x": "patient_demographics_other_fields", "other_fields_y": "patients_visits_other_fields"})
        merged_data = merged_data.rename(columns={"notes_x": "patient_lab_results_notes", "notes_y": "patient_medications_notes"})
        merged_data['age_group'] = merged_data['age'].apply(lambda x: 'UNKNOWN' if x == -1 else ('18-35' if x <= 35 else '36-65' if x <= 65 else '65+'))

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