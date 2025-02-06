from app import db, Dataset

def list_datasets():
    try:
        datasets = Dataset.query.all()
        print(f"Total datasets remaining: {len(datasets)}")
        print("\nSample of remaining datasets:")
        for dataset in datasets[:5]:  # Show first 5 datasets
            print(f"- {dataset.dataset_name}")
        
    except Exception as e:
        print(f"Error listing datasets: {str(e)}")

if __name__ == '__main__':
    list_datasets()
