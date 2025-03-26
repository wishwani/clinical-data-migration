<<<<<<< HEAD
# Clinical Study Data ETL Pipeline & Database Setup

## 1. Overview
This project processes synthetic clinical study data using an **ETL pipeline** built with Python and Pandas. The cleaned data is stored in a **PostgreSQL database** with a well-defined relational schema to support efficient querying and analysis.

## 2. Design Choices
### **ETL Pipeline Design**
- **Data Extraction**: Reads five CSV files containing patient demographics, visits, lab results, medications, and physician assignments.
- **Data Cleaning**:
  - Removes duplicates.
  - Handles missing values (e.g., missing ages set to `-1`, unknown genders labeled `UNKNOWN`).
  - Converts text fields to uppercase for consistency.
  - Parses date fields and replaces invalid values with a default date (`1900-01-01`).
- **Data Transformation**:
  - Merges related datasets using `patient_id` and `visit_id`.
  - Creates an `age_group` column for categorization.
- **Data Loading**: Saves the cleaned data into a PostgreSQL database.

### **Database Schema**
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
- Required Python libraries: `pandas`, `psycopg2`, `sqlalchemy`

### **Installation Steps**
1. **Clone the repository:**
   ```sh
   git clone https://github.com/wishwani/clinical-data-migration
   cd clinical-data-migration
   ```
2. **Install dependencies:**
   ```sh
   pip install pandas psycopg2 sqlalchemy
   ```
3. **Configure the database:**
   - Start PostgreSQL and create a database:
     ```sql
     CREATE DATABASE clinical_data;
     ```
   - Update database connection details in `config.py`:
     ```python
     DATABASE_URL = "postgresql://username:password@localhost/clinical_study"
     ```

## 4. Running the Code

### **Run the ETL Pipeline**
```sh
python etl_pipeline.py
```
This will:
- Load CSV files
- Clean and transform the data
- Save the cleaned dataset to `data/cleaned_data.csv`

### **Set Up the Database**
```sh
psql -U postgres -d clinical_data
\i 'C:/Users/Randika/MedDataSync/sql/schema.sql'
```
This will create the necessary tables in PostgreSQL.

### **Load Cleaned Data into PostgreSQL**
```sh
python load_to_db.py
```
This script reads `cleaned_data.csv` and inserts it into the database.

## 5. Querying the Database
Once the database is populated, you can run queries like:
```sql
SELECT * FROM patient_visits WHERE diagnosis LIKE '%diabetes%';
```

This project involves migrating clinical data from a PostgreSQL database to Supabase and performing SQL queries with Python integration. Below are the setup instructions, including how to install Supabase using Docker and initialize it.

## Prerequisites

1. **Docker**: You need Docker installed to run Supabase in a local environment.

   - To install Docker, follow the instructions from [Docker's official website](https://docs.docker.com/get-docker/).

2. **Supabase CLI**: You need to install the Supabase CLI to interact with the Supabase project.

   - To install the Supabase CLI, follow the instructions here: [Supabase CLI Documentation](https://supabase.com/docs/guides/cli).

## Steps to Set Up Supabase

1. **Install Docker**: If you haven't installed Docker, follow the steps from the Docker website linked above.

2. **Install Supabase CLI**:
   After installing Docker, you need the Supabase CLI to run Supabase locally. Install the CLI using the following commands:

   ```bash
   npm install -g supabase
For further installation details, visit Supabase CLI Installation.

Initialize Supabase: To initialize a Supabase project, navigate to the directory where you want to store your Supabase project and run:

bash
Copy
Edit
supabase init
This will create a new Supabase project in your directory.

Start Supabase: To start the Supabase services (including PostgreSQL and the Supabase API), use the following command:

bash
Copy
Edit
supabase start
This will start Supabase locally using Docker. Once the services are up, you can access your Supabase project at http://localhost:54321 in your browser.
This project provides a structured ETL pipeline and a relational database to manage clinical study data efficiently. The design ensures **data consistency, query optimization, and scalability** for further analysis and reporting.
=======
>>>>>>> 2514b172ed1e8ef2b5ddbd1ed9a828a283a4a2b8


