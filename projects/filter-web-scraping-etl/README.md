# Top Scrape \& Filter ETL



A small Python ETL project that scrapes movie ranking data from a Top 100 web page,

applies filtering rules, and stores the processed results in structured formats.



## What this project does



- Extracts movie title, release year, and ranking information

- Restricts the dataset to the top 25 entries

- Filters films released in the 2000s (year ≥ 2000)

- Saves the processed data to CSV and SQLite formats



## Technologies used



- Python
- requests
- BeautifulSoup
- pandas
- SQLite


## How to run


From the project root directory:



```bash

python src/scrape_top.py

```


## Output

Generated files are saved in the outputs/ directory:

CSV file containing the filtered movie data

SQLite database with the same dataset

