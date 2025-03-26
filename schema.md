erDiagram
    PATIENT_DEMOGRAPHICS ||--o{ PATIENT_VISITS : has
    PATIENT_VISITS ||--o{ PATIENT_LAB_RESULTS : includes
    PATIENT_VISITS ||--o{ PATIENT_MEDICATIONS : prescribes
    PATIENT_VISITS ||--o{ PHYSICIAN_ASSIGNMENTS : assigns

    PATIENT_DEMOGRAPHICS {
        STRING patient_id PK
        INT age
        STRING gender
        TEXT patient_demographics_other_fields
    }
    
    PATIENT_VISITS {
        STRING visit_id PK
        STRING patient_id FK
        DATE visit_date
        TEXT diagnosis
        STRING medication
        TEXT patients_visits_other_fields
    }

    PATIENT_LAB_RESULTS {
        STRING lab_test_id PK
        STRING patient_id FK
        STRING visit_id FK
        DATE test_date
        TEXT test_name
        FLOAT result_value
        STRING result_unit
        STRING reference_range
        TEXT notes
    }

    PATIENT_MEDICATIONS {
        STRING medication_id PK
        STRING patient_id FK
        STRING visit_id FK
        STRING medication
        TEXT dosage
        DATE start_date
        DATE end_date
        TEXT notes
    }

    PHYSICIAN_ASSIGNMENTS {
        STRING physician_id PK
        STRING patient_id FK
        STRING visit_id FK
        TEXT physician_name
        DATE assignment_date
        TEXT department
    }
