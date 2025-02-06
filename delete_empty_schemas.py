from app import db, SchemaInfo, Dataset
import json

def has_only_special_fields(schema_def):
    """Check if schema only contains special fields that should be ignored"""
    special_fields = {'domainMetadata', 'remove', 'metaData', 'txn', 'add', 'protocol'}
    return all(field in special_fields for field in schema_def.keys())

def delete_empty_schemas():
    try:
        empty_schema_paths = []
        
        # Get all schema info records
        all_schemas = SchemaInfo.query.all()
        
        for schema in all_schemas:
            try:
                # Parse schema definition
                schema_def = json.loads(schema.schema_definition)
                
                # Check if it's empty or only contains special fields
                if not schema_def or (
                    isinstance(schema_def, dict) and 
                    has_only_special_fields(schema_def)
                ):
                    empty_schema_paths.append(schema.dataset_path)
                    
            except json.JSONDecodeError:
                print(f"Warning: Invalid JSON in schema for path {schema.dataset_path}")
                continue
        
        print(f"Found {len(empty_schema_paths)} paths with empty schemas")
        if empty_schema_paths:
            print("Sample paths with empty schemas:")
            for path in empty_schema_paths[:5]:
                print(f"- {path}")
        
        # Delete from SchemaInfo table
        deleted_schemas = 0
        for path in empty_schema_paths:
            deleted = SchemaInfo.query.filter(
                SchemaInfo.dataset_path == path
            ).delete(synchronize_session='fetch')
            deleted_schemas += deleted
        
        # Delete from Dataset table where dataset_name matches any of the paths
        deleted_datasets = 0
        if empty_schema_paths:
            for path in empty_schema_paths:
                # Extract dataset name from path (last part after the last '/')
                dataset_name = path.rstrip('/').split('/')[-1]
                if dataset_name:
                    deleted = Dataset.query.filter(
                        Dataset.dataset_name == dataset_name
                    ).delete(synchronize_session='fetch')
                    deleted_datasets += deleted
        
        # Commit the changes
        db.session.commit()
        print(f"Successfully deleted {deleted_schemas} records from SchemaInfo table")
        print(f"Successfully deleted {deleted_datasets} records from Dataset table")
        
    except Exception as e:
        db.session.rollback()
        print(f"Error deleting records: {str(e)}")

if __name__ == '__main__':
    delete_empty_schemas()
