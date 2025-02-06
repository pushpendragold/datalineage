from app import db, Dataset

def delete_pump_datasets():
    try:
        # Delete records where dataset_name starts with 'pump'
        deleted = Dataset.query.filter(
            Dataset.dataset_name.like('pump%')
        ).delete(synchronize_session='fetch')
        
        # Commit the changes
        db.session.commit()
        print(f"Successfully deleted {deleted} records with dataset name starting with 'pump'")
        
    except Exception as e:
        db.session.rollback()
        print(f"Error deleting records: {str(e)}")

if __name__ == '__main__':
    delete_pump_datasets()
