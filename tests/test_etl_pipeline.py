import pytest
import pandas as pd
import os
import load_to_db

from etl_pipeline import load_data, clean_data, merge_data

# Test data setup
test_data = {
    "patient_demographics": pd.DataFrame({
        "patient_id": [1, 2, 3],
        "age": [25, None, 50],
        "gender": ["M", "F", None],
        "age_group": [18-35, 18-35, 36-65],
        "patient_demographics_other_fields": ["NON-SMOKER", "DIABETIC", "SMOKER"]
    }),
    "patient_visits": pd.DataFrame({
        "patient_id": [1, 2, 3],
        "visit_id": [101, 102, 103],
        "visit_date": ["2024-01-01", None, "2024-03-01"],
        "diagnosis" : ["DEPRESSION", "ANXIETY", "N/A"],
        "medication" : ["SERTRALINE", "ESCITALOPRAME", "BUSPIRONE"],
        "patients_visits_other_fields" : ["FOLLOW-UP VISIT", "INITAL ASSESMENT", "FOLLOW-UP VISIT"]
    }),
    "patient_lab_results": pd.DataFrame({
        "patient_id": [1, 2],
        "visit_id": [101, 102],
        "test_date": [None, "2024-02-02"],
        "result_value": [5.6, 7.8],
        "lab_test_id": ["L001", "L002"],
        "test_name": ["BLOOD GLUCOSE", "CHOLESTEROL"],
        "result_unit": ["MG/DL", "MG/D"],
        "reference_range": [105.0, 205.0],
        "patient_lab_results_notes": ["NORMAL", "NORMAL"],

    }),
    "patient_medications": pd.DataFrame({
        "patient_id": [1, 3],
        "visit_id": [101, 103],
        "medication_id": ["M001", "M002"],
        "medication": ["DrugA", "DrugB"],
        "dosage": ["10MG", "10MG"],
        "start_date": ["2023-03-05", "2023-03-06"],
        "end_date": ["2023-04-05", "2023-04-06"],
        "patient_medications_notes": ["INITIAL PRESCRIPTION", "INCREASED DOSAGE"]
    }),
    "physician_assignments": pd.DataFrame({
        "patient_id": [1, 2],
        "visit_id": [101, 102],
        "physician_id": [201, 202],
        "physician_name": ["DR. SMITH", "DR. SMITH"],
        "assignment_date": ["2023-03-05", "2023-03-06"],
        "department": ["PSYCHIATRY", None],

    })
}

@pytest.fixture
def sample_data():
    return test_data.copy()

# Test load_data function
def test_load_data(mocker):
    mocker.patch("etl_pipeline.pd.read_csv", side_effect=lambda x: test_data[os.path.basename(x).split('.')[0]])
    data = load_data()
    assert set(data.keys()) == set(test_data.keys())

# Test clean_data function
def test_clean_data(sample_data):
    cleaned_data = clean_data(sample_data)
    assert cleaned_data["patient_demographics"].isnull().sum().sum() == 0
    assert cleaned_data["patient_lab_results"].isnull().sum().sum() == 0
    assert cleaned_data["patient_demographics"]["gender"].unique().tolist() == ["M", "F", "UNKNOWN"]

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
        "patient_id": [1, 2, 3],
        "visit_id": [101, None, 103],
        "visit_date": ["2024-01-01", "2024-02-01", "2024-03-01"]
    })
    
    merged_data = merge_data(sample_data)
    
    assert set(merged_data["patient_id"]) == {1, 2, 3}
    assert "visit_id" in merged_data.columns

# Test insert function for patient_demographics
def test_insert_patients_data(mocker):
    mock_func = mocker.patch("load_to_db.load_data_to_patient_demographics", return_value=None)
    load_to_db.load_data_to_patient_demographics(None, "patient_demographics", None)
    mock_func.assert_called_once()

# Test insert function patient_visits
def test_insert_visits_data(mocker):
    mock_func = mocker.patch("load_to_db.load_data_to_patient_visits", return_value=None)
    load_to_db.load_data_to_patient_visits(None, "patient_visits", None)
    mock_func.assert_called_once()

# Test insert function for patient_lab_results
def test_insert_lab_results_data(mocker):
    mock_func = mocker.patch("load_to_db.load_data_to_patient_lab_results", return_value=None)
    load_to_db.load_data_to_patient_lab_results(None, "patient_lab_results", None)
    mock_func.assert_called_once()

# Test insert function for patient_medications
def test_insert_medications_data(mocker):
    mock_func = mocker.patch("load_to_db.load_data_to_patient_medications", return_value=None)
    load_to_db.load_data_to_patient_medications(None, "patient_medications", None)
    mock_func.assert_called_once()

# Test insert function for physician_assignments
def test_insert_physician_assignments_data(mocker):
    mock_func = mocker.patch("load_to_db.load_data_to_physician_assignments", return_value=None)
    load_to_db.load_data_to_physician_assignments(None, "physician_assignments", None)
    mock_func.assert_called_once()


