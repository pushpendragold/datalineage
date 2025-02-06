from app import db, SchemaInfo

def delete_pump_data():
    try:
        # Delete records where dataset_path contains 'pump'
        deleted = SchemaInfo.query.filter(
            SchemaInfo.dataset_path.like('%pump%')
        ).delete(synchronize_session='fetch')
        
        # Commit the changes
        db.session.commit()
        print(f"Successfully deleted {deleted} records with dataset paths containing 'pump'")
        
    except Exception as e:
        db.session.rollback()
        print(f"Error deleting records: {str(e)}")

if __name__ == '__main__':
    delete_pump_data()
