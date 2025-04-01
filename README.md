
# Clinical Study Data ETL Pipeline & Database Setup

## 1. Overview
This project processes synthetic clinical study data using an **ETL pipeline** built with Python and Pandas. The cleaned data is stored in a **PostgreSQL database** with a well-defined relational schema to support efficient querying and analysis.

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
- Corresponding `reference_range` values are adjusted using a transformation (e.g., `"1-5"` → `"100-5000"`).  

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
- Required Python libraries: `pandas`, `psycopg2`, `sqlalchemy`

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

## PostgreSQL:
- **Core System**: Relational database.
- **Installation**:
  - Local setup via package managers (e.g., `apt`, `brew`).
  - Cloud setup via managed services (e.g., Amazon RDS).
- **Real-Time**: Not natively supported, requires additional setup for real-time features.
- **Authentication**: Requires external configuration (e.g., OAuth, JWT).
- **Storage**: No built-in file storage; needs external setup.
- **Security**: Manages user roles and permissions directly through SQL commands.
- **Scaling**: Manual scaling required; you must configure replication and performance tuning.
- **Maintenance**: Manual database maintenance, backup, and tuning.

## Supabase:
- **Core System**: Built on top of PostgreSQL with additional features.
- **Installation**:
  - Self-hosted via Docker (requires `docker-compose`).
  - Managed hosting via Supabase platform (no manual setup required).
- **Configuration**:
  - Automatic setup of features like authentication, storage, and real-time subscriptions.
  - API automatically generated for database tables.
- **Real-Time**: Built-in real-time subscription support via `pg_subscription`.
- **Authentication**: Built-in authentication (JWT-based).
- **Storage**: Integrated file storage and management system.
- **Security**: Row-Level Security (RLS) enabled by default, integrated with authentication.
- **Scaling**: Auto-scaling and infrastructure management handled by Supabase.
- **Maintenance**: Managed infrastructure, including database scaling and backups.

## Summary Table:

| **Feature**               | **PostgreSQL**                             | **Supabase**                             |
|---------------------------|--------------------------------------------|------------------------------------------|
| **Core System**            | Relational database                        | Built on PostgreSQL with added features  |
| **API Generation**         | Manual or external tools                   | Automatic RESTful APIs generated        |
| **Authentication**         | Manual setup                               | Built-in authentication (JWT)           |
| **Real-Time**              | Not natively supported                     | Built-in real-time subscriptions        |
| **Storage**                | Requires external setup                    | Integrated file storage                 |
| **Row-Level Security**     | Manual setup                               | Enabled by default                       |
| **Infrastructure**         | Self-hosted or managed                     | Managed infrastructure by Supabase      |
| **Scaling**                | Manual scaling                             | Auto-scaling                             |
| **Maintenance**            | Manual maintenance                         | Managed infrastructure                  |

## Create a Schema in Supabase for data migration from postgress to supabase : 

create schema data_migration

## Run the Python script: For data loading to supabase
```
python python_integration.py
```
