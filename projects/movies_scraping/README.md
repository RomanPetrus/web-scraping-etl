# Movies Web Scraping ETL

A simple Python ETL pipeline that scrapes movie ranking data from the
web, processes it, and stores the results in CSV and SQLite formats.

## What this project does

-   Downloads an HTML page with movie rankings
-   Extracts movie title, release year, and ranking information
-   Cleans and validates the data
-   Saves the results to CSV and SQLite database files

## Technologies used

-   Python
-   requests
-   BeautifulSoup
-   pandas
-   SQLite

## How to run

Run the script:

``` bash
python src/scrape_movies.py
```

## Output

Generated files are saved in the `outputs/` directory: - CSV file with
extracted movie data - SQLite database containing the same data
