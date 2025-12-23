# Instructors SQLite

A small Python ETL project that loads instructor data from a CSV file into a
SQLite database and demonstrates basic SQL queries and data appending.

## What this project does

- Reads instructor data from a CSV file without headers
- Loads the data into a SQLite database table
- Executes basic SQL queries (SELECT, COUNT)
- Appends new records to an existing table


## Technologies used

- Python
- pandas
- SQLite


## How to run

From the project root directory:

```bash

python src/csv\_to\_sqlite.py

```

## Output

The script creates or updates a SQLite database file:
STAFF.db containing the INSTRUCTOR table with loaded and appended data

