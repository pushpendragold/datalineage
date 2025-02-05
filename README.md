# Data Pipeline Lineage Visualization

This project provides an interactive visualization of data pipeline lineage, showing the relationships between datasets and their upstream/downstream dependencies.

## Features

- Interactive graph visualization of dataset relationships
- Support for upstream and downstream connections
- SQLite database for storing dataset information
- Modern web interface using vis.js

## Setup

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

2. Run the application:
```bash
python app.py
```

3. Access the application at `http://localhost:5000`

## Database Schema

The application uses a SQLite database with the following schema:

Table: Dataset
- dataset_name (String): Name of the dataset
- type (String): Either 'upstream' or 'downstream'
- path (String): Actual path where the data is present

## Usage

1. Add your dataset information to the SQLite database
2. The visualization will automatically show the relationships between datasets
3. You can interact with the graph by:
   - Dragging nodes to rearrange the layout
   - Zooming in/out using the mouse wheel
   - Clicking on nodes to highlight connections
