# imports
from pathlib import Path
from datetime import datetime
import pandas as pd
from pandas.api.types import is_numeric_dtype
import sqlite3

# Path & Config
PROJECT_DIR = Path(__file__).resolve().parents[1]


OUTPUT_DIR = PROJECT_DIR / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR = PROJECT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

URL = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
RATES_URL = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"

LOG_PATH = LOG_DIR / "etl_process.log"
CSV_PATH = OUTPUT_DIR / "largest_banks_data.csv"
DB_PATH = OUTPUT_DIR / "banks.db"
TABLE_NAME ="largest_banks"
EXPECTED_COLUMNS = {
    "Bank name": "Name", 
    "Market cap (US$ billion)": "MC_USD_Billion"
    }
NA_MARKERS = ["N/A", "n/a", "NA", "", "null", "None"]

QUERIES = {
    "Top 5 banks (USD)": f"SELECT Name, MC_USD_Billion FROM {TABLE_NAME} LIMIT 5;",
    "Top 5 banks (EUR)": f"SELECT Name, MC_EUR_Billion FROM {TABLE_NAME} LIMIT 5;"
    }


def log_progress(message: str) -> None:
    """Append a timestamped-message to the log file."""
    timeformat = "%Y-%m-%d %H:%M:%S"
    timestamp = datetime.now().strftime(timeformat)
    with LOG_PATH.open('a', encoding="utf-8") as log:        
        log.write(f"{timestamp} - {message}\n")
    #print(f"{timestamp} - {message}")

# ETL functions
def extract(url: str) -> pd.DataFrame:
    """
    Extract the 'By market capitalization' table from the page and return a clean DataFrame
    with columns: Name, MC_USD_Billion
    """
    tables = pd.read_html(url, match="Market cap")
    if not tables:
        raise ValueError("Could not find the 'By market capitalization' table on the page.")
        
    df_raw = tables[0].copy()
    missing = set(EXPECTED_COLUMNS) - set(df_raw.columns)
    if missing:
        raise ValueError(f"Unexpected table schema. Missing columns: {missing}")
    
    df_raw = df_raw.rename(
        columns = EXPECTED_COLUMNS # Rename columns
    )
    log_progress("Data extraction complete. Initiating Transformation process")
    return df_raw  

def transform(df_raw: pd.DataFrame, rates_url: str) -> pd.DataFrame:
    """Reads exchange rates from a CSV file and adds converted market capitalization columns to the DataFrame."""
    df_raw = df_raw.copy()
    # Normalize invalid Name values to <NA>
    df_raw["Name"] = df_raw["Name"].str.strip().replace(NA_MARKERS, pd.NA)
    
    # Clean numeric column
    if not is_numeric_dtype(df_raw["MC_USD_Billion"]):
        df_raw["MC_USD_Billion"] = (df_raw["MC_USD_Billion"]
            .astype(str)
            .str.strip()
            .str.replace("\n", "", regex=False)
            .str.replace(",", "", regex=False)
            .pipe(pd.to_numeric, errors="coerce"))
        
    check_cols = list(EXPECTED_COLUMNS.values())
    invalid_mask = df_raw[check_cols].isna().any(axis=1)
    
    if invalid_mask.any():
        dropped = invalid_mask.sum()
        total = len(df_raw)
        
        print(f"Warning: dropping {dropped}/{total} invalid rows\n")
        print(df_raw.loc[invalid_mask])
        print("")
        df_clean = df_raw.loc[~invalid_mask].reset_index(drop=True).copy()
    else:
        df_clean = df_raw.copy()
    
    # Read the exchange rate CSV file
    rates = pd.read_csv(rates_url)    
    # Convert the contents to dictionary
    rates["Currency"] = rates["Currency"].astype(str).str.strip()
    rates["Rate"] = pd.to_numeric(rates["Rate"], errors="coerce")    
    rate_dict = dict(zip(rates["Currency"], rates["Rate"]))
    # Add one column per currency found in the CSV
    for currency, rate in rate_dict.items():
        if currency == "USD":
            continue
        
        col_name = f"MC_{currency.upper()}_Billion"
        df_clean[col_name] = (df_clean["MC_USD_Billion"]*float(rate)).round(2)

    log_progress("Data transformation complete. Initiating Loading process")
    #print (df_clean)
    return df_clean

def load_to_csv(df: pd.DataFrame, csv_path: Path) -> None:
    """Save the final data frame as a CSV file."""
    df.to_csv(csv_path, index=False)
    log_progress("Data saved to CSV file")

def load_to_db(df: pd.DataFrame, db_path: Path, table_name: str) -> None:
    """Save the final data frame to a database."""
    with sqlite3.connect(db_path) as con:
        df.to_sql(table_name, con, if_exists="replace", index=False)
    log_progress("Load DB: done (connection closed)")
    
def run_queries(queries: dict, db_path: Path) -> None:
    """Run the query on the database table and prints the output on the terminal."""
    log_progress("Queries: start")
    with sqlite3.connect(db_path) as con:
        for title, q in queries.items():
            df_res = pd.read_sql_query(q, con)
            print(f"\n --- {title} ---")
            print(df_res.to_string(index=False))
            
    log_progress("Queries: done (connection closed)")


def main() -> None:
    log_progress("Process: start")

    df_raw = extract(URL)
    df_clean = transform(df_raw, RATES_URL)
    load_to_csv(df_clean, CSV_PATH)
    load_to_db(df_clean, DB_PATH, TABLE_NAME)

    run_queries(QUERIES, DB_PATH) 
    log_progress("Process: complete")


if __name__ == "__main__":
    main()
