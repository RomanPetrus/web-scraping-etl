# -*- coding: utf-8 -*-
"""
Created on Mon Dec 22 17:09:23 2025

@author: petru
"""

import sqlite3
import pandas as pd

db_path = "outputs/Movies.db"
conn = sqlite3.connect(db_path)

tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)

print(tables)

df = pd.read_sql("SELECT * FROM Top_50 LIMIT 10;", conn)
print("\nSample rows:")
print(df)

count = pd.read_sql("SELECT COUNT(*) as n FROM Top_50;", conn)
print("\nRow count:")
print(count)