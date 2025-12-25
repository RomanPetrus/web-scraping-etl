# Instructors SQLite (ETL Mini Project)

A small Python ETL-style project that loads instructor data from a CSV
file into a SQLite database and demonstrates basic SQL operations.

## Project overview

This project simulates a simple data engineering task where structured
data is: - extracted from a CSV file, - loaded into a relational
database, - queried and extended with new records.

## What the project does

-   Reads instructor data from a CSV file without headers\
-   Creates or replaces a SQLite database table\
-   Executes basic SQL queries (`SELECT`, `COUNT`)\
-   Appends new records to an existing table

## Project structure

    instructors_sqlite/
    ├── data/
    │   └── INSTRUCTOR.csv
    ├── outputs/
    │   └── STAFF.db
    └── src/
        └── db_code.py

## Technologies used

-   Python
-   pandas
-   SQLite

## How to run

From the project root directory:

``` bash
python src/db_code.py
```

## Output

The script creates or updates a SQLite database file:

-   `STAFF.db`\
    containing the `INSTRUCTOR` table with loaded and appended data.
