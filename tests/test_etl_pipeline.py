from unittest.mock import patch
import pytest
import pandas as pd
import os

from etl_pipeline import load_data, clean_data, merge_data, modify_range
from load_to_postgresdb import load_data_to_table

# Test data setup
test_data = {
    "patient_demographics": pd.DataFrame({
        "patient_id": ['P1', 'P2', 'P3'],
        "age": [25, None, 50],
        "gender": ["MALE", "FEMALE", None],
        "age_group": ["18-35", "18-35", "36-65"],
        "patient_demographics_other_fields": ["NON-SMOKER", "DIABETIC", "SMOKER"]
    }),
    "patient_visits": pd.DataFrame({
        "patient_id": ['P1', 'P2', 'P3'],
        "visit_id": ['V101', 'V102', 'V103'],
        "visit_date": ["2024-01-01", None, "2024-03-01"],
        "diagnosis" : ["DEPRESSION", "ANXIETY", "N/A"],
        "medication" : ["SERTRALINE", "ESCITALOPRAME", "BUSPIRONE"],
        "patients_visits_other_fields" : ["FOLLOW-UP VISIT", "INITAL ASSESMENT", "FOLLOW-UP VISIT"]
    }),
    "patient_lab_results": pd.DataFrame({
        "patient_id": ['P1', 'P2'],
        "visit_id": ['V101', 'V102'],
        "test_date": ["2024-02-03", "2024-02-02"],
        "result_value": [15.6, 14.8],
        "lab_test_id": ["L001", "L002"],
        "test_name": ["BLOOD GLUCOSE", "CHOLESTEROL"],
        "result_unit": ["g/dl", "g/dl"],
        "reference_range": ["12-16", "12-16"],
        "patient_lab_results_notes": [None, None],

    }),
    "patient_medications": pd.DataFrame({
        "patient_id": ['P1', 'P3'],
        "visit_id": ['V101', 'V103'],
        "medication_id": ["M001", "M002"],
        "medication": ["DrugA", "DrugB"],
        "dosage": [10.0, 10.0],
        "start_date": ["2023-03-05", "2023-03-06"],
        "end_date": ["2023-04-05", "2023-04-06"],
        "patient_medications_notes": ["INITIAL PRESCRIPTION", "INCREASED DOSAGE"]
    }),
    "physician_assignments": pd.DataFrame({
        "patient_id": ['P1', 'P2'],
        "visit_id": ['V101', 'V102'],
        "physician_id": ['PH201', 'PH202'],
        "physician_name": ["DR. SMITH", "DR. SMITH"],
        "assignment_date": ["2023-03-05", "2023-03-06"],
        "department": ["PSYCHIATRY", None],

    })
}

# Define fixture to be reused across tests
@pytest.fixture
def sample_data():
    return test_data.copy()

# ----------------------
# Unit Tests -----------
# ----------------------

# Test whether load_data function returns all required DataFrames
def test_load_data(mocker):
    mocker.patch("etl_pipeline.pd.read_csv", side_effect=lambda x: test_data[os.path.basename(x).split('.')[0]])
    data = load_data()
    assert set(data.keys()) == set(test_data.keys())

# Test whether clean_data removes all nulls and normalizes gender values
def test_clean_data(sample_data):
    cleaned_data = clean_data(sample_data)
    assert cleaned_data["patient_demographics"].isnull().sum().sum() == 0
    assert cleaned_data["patient_visits"].isnull().sum().sum() == 0
    assert cleaned_data["patient_medications"].isnull().sum().sum() == 0
    assert cleaned_data["physician_assignments"].isnull().sum().sum() == 0
    assert cleaned_data["patient_demographics"]["gender"].unique().tolist() == ["MALE", "FEMALE", "UNKNOWN"]

# Test merge_data function
def test_merge_data(sample_data):
    merged_data = merge_data(sample_data)
    assert "patient_id" in merged_data.columns
    assert "visit_id" in merged_data.columns
    assert "lab_test_id" in merged_data.columns
    assert "medication_id" in merged_data.columns
    assert "physician_id" in merged_data.columns

# Test ensure patient_ids exist even if visit_id is missing
def test_merge_data_missing_values(sample_data):
    sample_data["patient_visits"] = pd.DataFrame({
        "patient_id": ['P1', 'P2', 'P3'],
        "visit_id": ['V101', None, 'V103'],
        "visit_date": ["2024-01-01", "2024-02-01", "2024-03-01"]
    })
    
    merged_data = merge_data(sample_data)
    
    assert set(merged_data["patient_id"]) == {'P1', 'P2', 'P3'}
    assert "visit_id" in merged_data.columns

# Test whether load_data_to_table internally calls to_sql correctly
@patch("pandas.DataFrame.to_sql")
def test_load_data_to_table_calls_to_sql(mock_to_sql):
    mock_df = pd.DataFrame({
        "patient_id": ["P1"],
        "age": [30],
        "age_group": ["18-35"],
        "gender": ["F"],
        "patient_demographics_other_fields": ["DATA"]
    })

    # Patch the global final_data used in load_to_postgresdb
    with patch("load_to_postgresdb.final_data", mock_df):
        table_name = "patient_demographics"
        columns = ["patient_id", "age", "age_group", "gender", "patient_demographics_other_fields"]
        dtypes = {
            "patient_id": str,
            "age": int,
            "age_group": str,
            "gender": str,
            "patient_demographics_other_fields": str
        }

        load_data_to_table(table_name, columns, dtypes, primary_keys=["patient_id"])

        mock_to_sql.assert_called_once()
        args, kwargs = mock_to_sql.call_args
        assert kwargs["if_exists"] == "replace"
        assert kwargs["index"] is False
        assert kwargs["dtype"] == dtypes

# Test result_unit conversion to uppercase
def test_result_unit_uppercase():
    df = test_data["patient_lab_results"].copy()
    df["result_unit"] = df["result_unit"].str.upper()
    df.loc[df["result_unit"] == "G/DL", "result_unit"] = "MG/DL"
    assert all(df["result_unit"] == "MG/DL")

# Test correct conversion of values and reference ranges for G/DL to MG/DL
def test_update_g_dl_unit():
    df = test_data["patient_lab_results"].copy()
    if "result_unit" in df.columns and "reference_range" in df.columns:
        g_dl_condition = df["result_unit"] == "G/DL"
        df.loc[g_dl_condition, "result_value"] *= 1000
        df.loc[g_dl_condition, "result_unit"] = "MG/DL"
        df.loc[g_dl_condition, "reference_range"] = df.loc[g_dl_condition, "reference_range"].apply(modify_range)
    assert df["result_unit"].iloc[0] == "MG/DL"
    assert df["result_value"].iloc[0] == 15600  
    assert df["reference_range"].iloc[0] == "12000-16000" 

# ----------------------
# Lab Notes Logic Tests
# ----------------------

# Test notes update based on result_value and reference_range
def test_update_notes():
    df = test_data["patient_lab_results"].copy()
    df["result_value"] = [1.7, 1.8]  
    df["reference_range"] = ["1.5-2.0", "1.5-2.0"] 
    df = apply_lab_notes(df)
    assert all(df["patient_lab_results_notes"] == "NORMAL")

# Test notes update for LOW result_value
def test_update_notes_low():
    df = test_data["patient_lab_results"].copy()
    df["result_value"] = [0.7, 0.8]  
    df["reference_range"] = ["1.5-2.0", "1.5-2.0"] 
    df = apply_lab_notes(df)
    assert df["patient_lab_results_notes"].iloc[0] == "LOW"

# Test notes update for HIGH result_value
def test_update_notes_high():
    df = test_data["patient_lab_results"].copy()
    df["result_value"] = [2.7, 2.8] 
    df["reference_range"] = ["1.5-2.0", "1.5-2.0"]  
    df = apply_lab_notes(df)
    assert df["patient_lab_results_notes"].iloc[0] == "HIGH"

# Helper function to assign lab notes (NORMAL/LOW/HIGH) based on range
def apply_lab_notes(df):
    for index, row in df.iterrows():
        if pd.notnull(row["result_value"]) and "reference_range" in df.columns:
            min_range, max_range = map(float, row["reference_range"].split('-'))
            if pd.isnull(row["patient_lab_results_notes"]):
                if min_range <= row["result_value"] <= max_range:
                    df.at[index, "patient_lab_results_notes"] = "NORMAL"
                elif row["result_value"] < min_range:
                    df.at[index, "patient_lab_results_notes"] = "LOW"
                elif row["result_value"] > max_range:
                    df.at[index, "patient_lab_results_notes"] = "HIGH"
    return df

# Ensure modify_range multiplies ranges by 1000
def test_modify_range():
    assert modify_range("12-16") == "12000-16000"
    assert modify_range("10-20") == "10000-20000"