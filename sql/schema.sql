CREATE TABLE IF NOT EXISTS patient_demographics (
    patient_id VARCHAR(10) PRIMARY KEY,
    age INT CHECK (age >= -1),
    age_group VARCHAR(10) CHECK (age_group IN ('18<','18-35', '36-65', '65+')),
    gender VARCHAR(10) CHECK (gender IN ('MALE', 'FEMALE', 'UNKNOWN')),
    patient_demographics_other_fields TEXT
);

CREATE TABLE IF NOT EXISTS patient_visits (
    patient_id VARCHAR(10) NOT NULL,
    visit_id VARCHAR(10) PRIMARY KEY,
    visit_date DATE NOT NULL CHECK (visit_date <= CURRENT_DATE),
    diagnosis TEXT NOT NULL,
    medication TEXT,
    patients_visits_other_fields TEXT,
    FOREIGN KEY (patient_id) REFERENCES patient_demographics(patient_id) ON DELETE CASCADE
);

CREATE INDEX idx_patient_visits_patient ON patient_visits(patient_id);
CREATE INDEX idx_patient_visits_visit ON patient_visits(visit_id);

CREATE TABLE IF NOT EXISTS patient_lab_results (
    patient_id VARCHAR(10) NOT NULL,
    lab_test_id VARCHAR(10) PRIMARY KEY,
    visit_id VARCHAR(10) NOT NULL,
    test_date DATE NOT NULL CHECK (test_date <= CURRENT_DATE), 
    test_name TEXT NOT NULL,
    result_value FLOAT,
    result_unit VARCHAR(20),
    reference_range VARCHAR(50),
    patient_lab_results_notes TEXT,
    FOREIGN KEY (patient_id) REFERENCES patient_demographics(patient_id) ON DELETE CASCADE,
    FOREIGN KEY (visit_id) REFERENCES patient_visits(visit_id) ON DELETE CASCADE
);

CREATE INDEX idx_lab_results_patient ON patient_lab_results(patient_id);
CREATE INDEX idx_lab_results_visit ON patient_lab_results(visit_id);
CREATE INDEX idx_lab_results_test_date ON patient_lab_results(test_date);

CREATE TABLE IF NOT EXISTS patient_medications (
    patient_id VARCHAR(10) NOT NULL,
    medication_id VARCHAR(10) PRIMARY KEY,
    visit_id VARCHAR(10) NOT NULL,
    medication TEXT NOT NULL,
    dosage TEXT NOT NULL,
    start_date DATE NOT NULL CHECK (start_date <= CURRENT_DATE),
    end_date DATE CHECK (end_date >= start_date), 
    patient_medications_notes TEXT,
    FOREIGN KEY (patient_id) REFERENCES patient_demographics(patient_id) ON DELETE CASCADE,
    FOREIGN KEY (visit_id) REFERENCES patient_visits(visit_id) ON DELETE CASCADE
);

CREATE INDEX idx_medications_patient ON patient_medications(patient_id);
CREATE INDEX idx_medications_visit ON patient_medications(visit_id);

CREATE TABLE IF NOT EXISTS physician_assignments (
    patient_id VARCHAR(10) NOT NULL,
    visit_id VARCHAR(10) NOT NULL,
    physician_id VARCHAR(10),
    physician_name TEXT NOT NULL,
    assignment_date DATE NOT NULL CHECK (assignment_date <= CURRENT_DATE),
    department TEXT NOT NULL,
    PRIMARY KEY (patient_id, visit_id, physician_id),
    FOREIGN KEY (patient_id) REFERENCES patient_demographics(patient_id) ON DELETE CASCADE,
    FOREIGN KEY (visit_id) REFERENCES patient_visits(visit_id) ON DELETE CASCADE
);

CREATE INDEX idx_physician_patient ON physician_assignments(patient_id);
CREATE INDEX idx_physician_visit ON physician_assignments(visit_id);

