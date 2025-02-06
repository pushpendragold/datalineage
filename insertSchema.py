import pandas as pd
from app import db, Dataset, SchemaInfo
import json

# Clear existing data
db.session.query(SchemaInfo).delete()
db.session.commit()

# Load the Excel file
file_path = "Schema.xlsx"  # Update with the correct path if needed
df = pd.read_excel(file_path)

# Insert data using SQLAlchemy
for _, row in df.iterrows():
    schema_info = SchemaInfo(
        dataset_path=row['path'],
        schema_definition=row['schema']
    )
    db.session.add(schema_info)

# Commit the changes
db.session.commit()

print("Data inserted successfully!")
