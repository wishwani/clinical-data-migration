import pandas as pd
import numpy as np

# Data Extraction: Load CSV files into pandas DataFrames
patient_data = pd.read_csv('data/patient_data.csv')
visit_data = pd.read_csv('data/visit_data.csv')
medication_data = pd.read_csv('data/medication_data.csv')

# 1. Data Cleaning

# Handle missing values
# For simplicity, let's fill missing age with the median value and gender with 'Unknown'.
patient_data['age'].fillna(patient_data['age'].median(), inplace=True)
patient_data['gender'].fillna('Unknown', inplace=True)

# Handle missing visit dates in visit_data
visit_data['visit_date'].fillna(pd.to_datetime('1900-01-01'), inplace=True)

# Handle missing diagnosis by filling with 'No Diagnosis'
visit_data['diagnosis'].fillna('No Diagnosis', inplace=True)

# Remove duplicate records
patient_data.drop_duplicates(inplace=True)
visit_data.drop_duplicates(inplace=True)
medication_data.drop_duplicates(inplace=True)

# Normalize inconsistent formatting (e.g., date formats, text case)
# Convert all column names to lowercase for consistency
patient_data.columns = patient_data.columns.str.lower()
visit_data.columns = visit_data.columns.str.lower()
medication_data.columns = medication_data.columns.str.lower()

# Normalize text data (e.g., capitalize gender)
patient_data['gender'] = patient_data['gender'].str.capitalize()

# 2. Data Transformation & Merging

# Merging datasets based on common keys
# Merge visit_data with patient_data on patient_id
merged_data = pd.merge(visit_data, patient_data, on='patient_id', how='left')

# Merge the result with medication_data on visit_id
final_data = pd.merge(merged_data, medication_data, on='visit_id', how='left')

# Create new derived fields
# Example: Create an 'age_group' field based on age
final_data['age_group'] = pd.cut(final_data['age'], 
                                  bins=[0, 18, 35, 50, 65, np.inf], 
                                  labels=['0-18', '19-35', '36-50', '51-65', '65+'])

# Example: Add a 'visit_frequency' field - counting number of visits per patient
visit_counts = final_data.groupby('patient_id').size().reset_index(name='visit_frequency')
final_data = pd.merge(final_data, visit_counts, on='patient_id', how='left')

# 3. Documentation: Output the cleaned and transformed data

# Save the cleaned data to a new CSV file
final_data.to_csv('cleaned_transformed_data.csv', index=False)

# Print the first few rows of the final data for verification
print(final_data.head())
