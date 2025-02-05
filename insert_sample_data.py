from app import db, Dataset, SchemaInfo
import json

# Clear existing data
db.session.query(Dataset).delete()
db.session.query(SchemaInfo).delete()
db.session.commit()

# Define the lineage structure
lineage = [
    # First path: bronze1 -> silver1 -> gold1
    {'name': 'DataSetGold1', 'type': 'downstream', 'path': '/gold/path1/'},
    {'name': 'DataSetGold1', 'type': 'upstream', 'path': '/silver/path1/'},
    {'name': 'DataSetSilver1', 'type': 'downstream', 'path': '/silver/path1/'},
    {'name': 'DataSetSilver1', 'type': 'upstream', 'path': '/bronze/path1/'},
    
    # Second path: bronze2 -> silver2 -> gold2
    {'name': 'DatasetGold2', 'type': 'downstream', 'path': '/gold/path2/'},
    {'name': 'DatasetGold2', 'type': 'upstream', 'path': '/silver/path2/'},
    {'name': 'DataSetSilver2', 'type': 'downstream', 'path': '/silver/path2/'},
    {'name': 'DataSetSilver2', 'type': 'upstream', 'path': '/bronze/path2/'},
    
    # Third path: bronze3 -> silver3 -> gold3
    {'name': 'DatasetGold3', 'type': 'downstream', 'path': '/gold/path3/'},
    {'name': 'DatasetGold3', 'type': 'upstream', 'path': '/silver/path3/'},
    {'name': 'DataSetSilver3', 'type': 'downstream', 'path': '/silver/path3/'},
    {'name': 'DataSetSilver3', 'type': 'upstream', 'path': '/bronze/path3/'},
    
    # Fourth path: bronze1 -> silver4.0 -> gold4
    {'name': 'DataSetGold4', 'type': 'downstream', 'path': '/gold/path4/'},
    {'name': 'DataSetGold4', 'type': 'upstream', 'path': '/silver/path4.0/'},
    {'name': 'DataSetSilver4', 'type': 'downstream', 'path': '/silver/path4.0/'},
    {'name': 'DataSetSilver4', 'type': 'upstream', 'path': '/bronze/path1/'},
    
    # Fifth path: bronze2 -> silver4.1 -> gold5
    {'name': 'DatasetGold5', 'type': 'downstream', 'path': '/gold/path5/'},
    {'name': 'DatasetGold5', 'type': 'upstream', 'path': '/silver/path4.1/'},
    {'name': 'DataSetSilver4', 'type': 'downstream', 'path': '/silver/path4.1/'},
    {'name': 'DataSetSilver4', 'type': 'upstream', 'path': '/bronze/path2/'},
    
    # Sixth path: bronze3 -> silver4.2 -> gold6
    {'name': 'DatasetGold6', 'type': 'downstream', 'path': '/gold/path6/'},
    {'name': 'DatasetGold6', 'type': 'upstream', 'path': '/silver/path4.2/'},
    {'name': 'DataSetSilver4', 'type': 'downstream', 'path': '/silver/path4.2/'},
    {'name': 'DataSetSilver4', 'type': 'upstream', 'path': '/bronze/path3/'}
]

# Add datasets
for dataset in lineage:
    db.session.add(Dataset(
        dataset_name=dataset['name'],
        type=dataset['type'],
        path=dataset['path']
    ))

# Sample schema information for each path
paths = [
    '/bronze/path1/', '/bronze/path2/', '/bronze/path3/',
    '/silver/path1/', '/silver/path2/', '/silver/path3/',
    '/silver/path4.0/', '/silver/path4.1/', '/silver/path4.2/',
    '/gold/path1/', '/gold/path2/', '/gold/path3/',
    '/gold/path4/', '/gold/path5/', '/gold/path6/'
]

# Add basic schema information for each path
for path in paths:
    layer = path.split('/')[1]  # bronze, silver, or gold
    schema = {
        'id': {
            'type': 'integer',
            'description': 'Unique identifier for the record'
        },
        f'{layer}_timestamp': {
            'type': 'timestamp',
            'description': f'Timestamp when the {layer} record was created'
        },
        f'{layer}_value': {
            'type': 'string',
            'description': f'Main value field for the {layer} record'
        }
    }
    
    db.session.add(SchemaInfo(
        dataset_path=path,
        schema_definition=json.dumps(schema)
    ))

db.session.commit()

print("Sample data and schema information has been inserted successfully!")
print(f"Total paths: {len(paths)}")
print(f"Total datasets: {len(lineage)}")
print(f"Total schemas: {len(paths)}")