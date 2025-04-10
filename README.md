
# Clinical Study Data ETL Pipeline & Database Setup

## 1. Overview
This project processes synthetic clinical study data using an **ETL pipeline** built with Python and Pandas. The cleaned data is stored in a **PostgreSQL database** and **Supabase** with a well-defined relational schema to support efficient querying and analysis.

## 2. Design Choices
### **ETL Pipeline Design**
### Data Extraction: 
- Reads five CSV files containing patient demographics, visits, lab results, medications, and physician assignments.
### Data Cleaning:
### 1. Duplicate Records  
- Duplicate rows within each dataset are removed to ensure data integrity.  

### 2. Handling Missing Values  
- Missing numerical values are replaced with `-999` as a placeholder for further analysis.  
- Missing categorical values are replaced with `"UNKNOWN"` and converted to uppercase.  
- Missing `age` values are filled with the median age of the dataset.  

### 3. Standardizing Dosage  
- Medication `dosage` values containing `"mg"` are converted to numerical format and stored in a new column named `dosage_mg`.  

### 4. Standardizing Laboratory Test Results  
- `result_unit` values are converted to uppercase for consistency.  
- If `result_unit` is `"G/DL"`, it is converted to `"MG/DL"` by multiplying `result_value` by `1000`.  
- Corresponding `reference_range` values are adjusted using a transformation (e.g., `"1-5"` → `"1000-5000"`).  

### 5. Interpreting Lab Results  
- If `notes` are missing in `patient_lab_results`, they are updated based on `result_value` and `reference_range`:  
  - **"NORMAL"** → if the result is within range.  
  - **"LOW"** → if the result is below range.  
  - **"HIGH"** → if the result is above range.  

### 6. Date Formatting  
- Date columns (`test_date`, `start_date`, `end_date`, `assignment_date`) are converted to the format **YYYY-MM-DD**.  


### Data Transformation:
### Merges related datasets using `patient_id` and `visit_id`.
### Creates an `age_group` column for categorization.
  - `"18-35"` → for patients aged 18-35  
  - `"36-65"` → for patients aged 36-65  
  - `"65+"` → for patients older than 65 
### Visit Frequency Calculation  
  - The number of visits per patient is computed from the `patient_visits` dataset and merged into the final dataset.   

### Database Schema
The database follows a **relational design** to maintain **data integrity** and enable **efficient querying**.

#### **Tables:**
1. `patient_demographics` - Stores patient information.
2. `patient_visits` - Records visits and diagnoses.
3. `patient_lab_results` - Stores test results.
4. `patient_medications` - Tracks prescribed medications.
5. `physician_assignments` - Links physicians to patient visits.

Indexes are created on **foreign keys** and frequently queried columns for **performance optimization**.

## 3. Setup Instructions

### **Prerequisites**
- Python 3.x
- PostgreSQL
- Docker
- Supabase
- Required Python libraries: `pandas`, `psycopg2`, `sqlalchemy`, `matplotlib` , `seaborn`

### **Installation Steps**
1. **Clone the repository:**
   ```
   git clone https://github.com/wishwani/clinical-data-migration.git
   cd clinical-data-migration
   ```
2. **Install dependencies:**
   ```
   pip install pandas psycopg2 sqlalchemy
   ```

## 3. Running the Code
### **Run the ETL Pipeline**
```
python etl_pipeline.py
```
This will:
- Load CSV files
- Clean and transform the data
- Save the cleaned dataset to `data/cleaned_data.csv`
### **Set Up the Database**

### **1. Install PostgreSQL**  
If you haven't installed PostgreSQL yet, download and install it from the official website:  
[PostgreSQL Downloads](https://www.postgresql.org/download/)

### **2. Create the Postgres Database**  
Open the PostgreSQL command line (`psql`) and give the password for user postgres

Run the following command to create a database: 

```
CREATE DATABASE clinical_data;
```

### **3. Connect to the database**
```
psql -U postgres -d clinical_data
```
### **4. Execute the schema file**
```
\i 'C:/Users/Randika/MedDataSync/sql/schema.sql' (Use your project location for schema.sql)
```
This will create the necessary tables in PostgreSQL.

### **5. Load Cleaned Data into PostgreSQL**
```
python load_to_postgresdb.py
```
This script reads `cleaned_data.csv` and inserts it into the postgres database.
 
## 5. Supabase Setup and Migration

## Prerequisites
1. **Docker**: You need Docker installed to run Supabase in a local environment.
   - To install Docker, follow the instructions from [Docker's official website](https://docs.docker.com/get-docker/).
2. **Supabase CLI**: You need to install the Supabase CLI to interact with the Supabase project.
   - To install the Supabase CLI, follow the instructions here: [Supabase CLI Documentation](https://supabase.com/docs/guides/cli).
### Steps to Set Up Supabase
1. **Install Docker**: If you haven't installed Docker, follow the steps from the Docker website linked above.
2. **Install Supabase CLI**:
   After installing Docker, you need the Supabase CLI to run Supabase locally. Install the CLI using the following commands:
   ```
   npm install -g supabase
   ```
For further installation details, visit Supabase CLI Installation.

3. **Initialize Supabase**: 
navigate to the directory where you want to store your Supabase project and run:
```
supabase init
```
This will create a new Supabase project in your directory.
Start Supabase: To start the Supabase services (including PostgreSQL and the Supabase API), use the following command:
```
supabase start
```
This will start Supabase locally using Docker. Once the services are up, you can access your Supabase project at http://localhost:54323/project/default in your browser.

```
python load_to_supabasedb.py
```
This script reads `cleaned_data.csv` and inserts it into the supabase database(public schema).

# Differences in Configuration and Setup: PostgreSQL vs Supabase

## Overview

| Feature / Config             | **Supabase Local** (`supabase start`)        | **Traditional PostgreSQL (Local)**          |
|-----------------------------|----------------------------------------------|---------------------------------------------|
| **Port**                    | `54322` (Docker-mapped port)                 | `5432` (default)                            |
| **User / Password**         | `postgres` / `postgres` (default)            | Set manually during installation            |
| **Access Method**           | Docker container via Supabase CLI            | Native or Docker-based access               |
| **Admin UI**                | Supabase Studio (`http://localhost:54323`)   | `psql`, pgAdmin, DBeaver, etc.              |
| **Schema Initialization**   | Auto-created schemas with RLS, auth, etc.    | Manual schema creation                      |
| **Extensions**              | Many pre-enabled (`uuid-ossp`, `pg_net`, etc.)| Minimal, added manually                     |
| **Migration Support**       | Built-in via `supabase/migrations` folder    | External tools (Flyway, Alembic, etc.)      |
| **Row-Level Security (RLS)**| Enabled by default                           | Disabled by default                         |
| **Realtime Support**        | Built-in via Supabase Realtime               | Not available by default                    |
| **REST/GraphQL APIs**       | Auto-generated from tables                   | Not 

---

## Create a Schema in Supabase for data migration from postgress to supabase : 

create schema data_migration

## Run the Python script: For data loading to supabase
```
python python_integration.py
```

# Run the test cases

```
python -m pytest tests/test_etl_pipeline.py
```
