import sqlite3
import pandas as pd
from pathlib import Path

# --- PATHS ---
CODE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = CODE_DIR.parent
DATA_DIR = PROJECT_DIR/"data"
    
OUTPUT_DIR = PROJECT_DIR/"outputs" # Project output directory path
OUTPUT_DIR.mkdir(exist_ok=True)

DB_PATH = OUTPUT_DIR/"STAFF.db"
CSV_PATH = DATA_DIR / "INSTRUCTOR.csv"

# ---CONSTANTS---
TABLE_NAME = 'INSTRUCTOR'
ATTRIBUTE_LIST = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

# Data to append
data_dict = {'ID' : [100],
            'FNAME' : ['John'],
            'LNAME' : ['Doe'],
            'CITY' : ['Paris'],
            'CCODE' : ['FR']}
data_append = pd.DataFrame(data_dict)

# ---- ETL FUNCTIONS ----
def extract(csv_path) -> pd.DataFrame:
    """Read the CSV data into a DataFrame."""
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")
        
    df = pd.read_csv(CSV_PATH, names = ATTRIBUTE_LIST)
    return df

def load_sql(df:pd.DataFrame, db_path:Path, table_name:str, if_exists_mode:str) -> None:
    """Save DataFrame to SQLite."""
    with sqlite3.connect(db_path) as conn: # Connect to the SQLite3 service
        df.to_sql(table_name, conn, if_exists=if_exists_mode, index=False)
        print(f"DB write done: if_exists='{if_exists_mode}'")       
    
def read_db(db_path:Path, table_name:str, selected_columns:str="*") -> pd.DataFrame:
    """Run a SELECT query and return the result as a DataFrame."""    
    #columns_sql = ", ".join(selected_columns)
    
    query = f"SELECT {selected_columns} FROM {table_name}"
    with sqlite3.connect(db_path) as conn:
        output = pd.read_sql(query, conn)
    print(query)
    print(output)
    return output
        
   
def main() -> None:
    df = extract(CSV_PATH)
    print("Preview of dataframe:")
    print(df.head())
    
    load_sql(df, DB_PATH, TABLE_NAME, "replace")
    
    # Query 1: Display all rows of the table
    read_db(DB_PATH, TABLE_NAME, '*')
    
    # Query 2: Display only the FNAME column for the full table
    read_db(DB_PATH, TABLE_NAME, 'FNAME')
    
    # Query 3: Display the count of the total number of rows
    read_db(DB_PATH, TABLE_NAME, "COUNT(*) AS n")
    load_sql(data_append, DB_PATH, TABLE_NAME, "append")
    
    # Query 4: Display the count of the total number of rows
    read_db(DB_PATH, TABLE_NAME, "COUNT(*) AS n")
    
    
if __name__ == "__main__":
    main()
    