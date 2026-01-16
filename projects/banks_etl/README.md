# Banks ETL Pipeline

## Overview
ETL pipeline that extracts the largest banks by market capitalization, cleans the dataset,
enriches it using exchange rates, and stores results in CSV and SQLite.

## What it does
1. Extract table from an archived Wikipedia page
2. Clean and validate numeric market cap values
3. Add converted market cap columns for multiple currencies (USD skipped)
4. Save outputs to CSV and SQLite
5. Run example SQL queries

## How to run
```bash
pip install -r requirements.txt
python projects/banks_etl/src/banks_etl.py
```

```text
projects/banks_etl/
├─ src/banks_etl.py
├─ notebooks/banks_etl_exploration.ipynb
├─ outputs/
├─ logs/
└─ README.md
```