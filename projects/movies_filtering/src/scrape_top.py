import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path


# ---- CONSTANTS ----
URL = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
BASE_DIR = Path(__file__).resolve().parent.parent # Project root directory path
OUTPUT_DIR = BASE_DIR/"outputs" # Project output directory path
OUTPUT_DIR.mkdir(exist_ok=True) # Creates the output directory if it does not already exist

CSV_PATH = OUTPUT_DIR/"N_films.csv"
DB_NAME = OUTPUT_DIR/"N_Movies.db"

TABLE_NAME = 'Top_25'
TOP_N = 25

COLUMNS = ["Film", "Year", "Rotten Tomatoes' Top 100"]

# ---- ETL FUNCTIONS ----
def extract(url: str)-> str:
    """Download page HTML and return it as text"""
    response = requests.get(
        url,
        timeout=20, # Set a maximum wait time for the HTTP request
        headers={"User-Agent": "Mozilla/5.0"} # Identify the request as coming from a browser
        )
    response.raise_for_status() # Fail fast on HTTP errors (e.g. 404, 500)
    return response.text

    
def transform(html: str, top_n: int = TOP_N) -> pd.DataFrame:
    """Parse HTML, extract top_n rows, return DataFrame"""
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("tbody")
    if not tables:
        raise ValueError("No tables found on the page") # Fail fast if the expected table structure is missing
        
    rows = tables[0].find_all("tr") # <tr> — table row    
    records = []
    
    for row in rows:
        col = row.find_all("td") # <td> — table data
        if len(col) < len(COLUMNS):
            continue
        year = pd.to_numeric(col[2].get_text(strip=True), errors="coerce")
        if pd.isna(year):
            print(year)
            continue

        if  2000 <= year <= 2009:
            data_dict = {
                COLUMNS[0]: col[1].get_text(strip=True),
                COLUMNS[1]: year,
                COLUMNS[2]: col[3].get_text(strip=True)
                }
            records.append(data_dict)              
        
        if len(records) == top_n:
            break
        
    df = pd.DataFrame(records, columns=COLUMNS)
  
    return df            

def load_csv(df:pd.DataFrame, csv_path: str) -> None:
    """Save DataFrame to CSV"""
    df.to_csv(csv_path, index=False)
    
def load_sql(df: pd.DataFrame, db_name: str, table_name: str) -> None:
    """Save DataFrame to SQLite"""
    with sqlite3.connect(db_name) as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
       
def main() -> None:
    html = extract(URL)
    df = transform(html, TOP_N)
    print(df.head())
    
    load_csv(df, CSV_PATH)
    load_sql(df, DB_NAME, TABLE_NAME)
    
if __name__ == "__main__":
    main()



