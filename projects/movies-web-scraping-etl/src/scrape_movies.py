import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup


# ---- CONSTANTS ----
URL = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
DB_NAME = 'outputs/Movies.db'
TABLE_NAME = 'Top_50'
CSV_PATH = 'outputs/N_films.csv'
TOP_N = 50

COLUMNS = ["Average Rank", "Film", "Year"]

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
        data_dict = {
            COLUMNS[0]: col[0].get_text(strip=True),
            COLUMNS[1]: col[1].get_text(strip=True),
            COLUMNS[2]: col[2].get_text(strip=True)
            }
        records.append(data_dict)        
        
        if len(records) == top_n:
            break
        
    df = pd.DataFrame(records, columns=COLUMNS)
    # Convert numeric columns to integers (fail-safe conversion)
    df[COLUMNS[0]] = pd.to_numeric(df[COLUMNS[0]], errors="coerce").astype("Int64")
    df[COLUMNS[2]] = pd.to_numeric(df[COLUMNS[2]], errors="coerce").astype("Int64")
    
    # Remove rows with missing required numeric values and reset row indexing
    df = df.dropna(subset=[COLUMNS[0], COLUMNS[2]]).reset_index(drop=True)
    # dropna removes rows with missing values
    # subset defines which columns are checked for missing values
    # reset_index(drop=True) resets row numbering after rows were removed
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



