import sqlite3
import pandas as pd

from app import db, Dataset, SchemaInfo
import json

# Clear existing data
db.session.query(Dataset).delete()
db.session.commit()

# Load the Excel file
file_path = "DAGs_io_paths.xlsx"  # Update with the correct path if needed
df = pd.read_excel(file_path)

# Connect to SQLite database (or create it)
conn = sqlite3.connect("lineage.db")
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS Dataset (
    dataset_name TEXT,
    type TEXT,
    path TEXT
)
""")

# Insert data into the table
for _, row in df.iterrows():
    cursor.execute("INSERT INTO Dataset (dataset_name, type, path) VALUES (?, ?, ?)", (row['name'], row['type'], row['path']))

# Commit and close the connection
conn.commit()
conn.close()

print("Data inserted successfully!")
