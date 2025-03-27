import pytest
import pandas as pd
import os
import sys

# Ensure the root directory is added to the sys.path so that the modules can be found
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from etl_pipeline import load_data, clean_data, merge_data
from load_to_db import insert_patients_data, insert_visits_data, insert_lab_results_data, insert_medications_data, insert_physician_assignments_data
from unittest.mock import MagicMock

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
    assert "age_group" in merged_data.columns
    assert "medication" in merged_data.columns
    assert "physician_id" in merged_data.columns

# Test database insert functions
def test_insert_patients_data(mocker):
    cursor_mock = MagicMock()
    insert_patients_data(cursor_mock, test_data["patient_demographics"])
    cursor_mock.executemany.assert_called()

def test_insert_visits_data(mocker):
    cursor_mock = MagicMock()
    insert_visits_data(cursor_mock, test_data["patient_visits"])
    cursor_mock.executemany.assert_called()

def test_insert_lab_results_data(mocker):
    cursor_mock = MagicMock()
    insert_lab_results_data(cursor_mock, test_data["patient_lab_results"])
    cursor_mock.executemany.assert_called()

def test_insert_medications_data(mocker):
    cursor_mock = MagicMock()
    insert_medications_data(cursor_mock, test_data["patient_medications"])
    cursor_mock.executemany.assert_called()

def test_insert_physician_assignments_data(mocker):
    cursor_mock = MagicMock()
    insert_physician_assignments_data(cursor_mock, test_data["physician_assignments"])
    cursor_mock.executemany.assert_called()


