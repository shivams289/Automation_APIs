import pandas as pd
from app import create_app, db
from app.model import PerformanceHistory
def insert_data_from_csv(file_path):
    # Create the Flask application context
    app = create_app()
    with app.app_context():
        # Read the CSV file into a Pandas DataFrame
        df = pd.read_csv(file_path)
        
        # Iterate over each row in the DataFrame
        for index, row in df.iterrows():
            # Create a User object with data from each row
            user = PerformanceHistory(date=row['dates'], scheme_id=row['scheme_id'], nav= row['nav'])
            # Add the User object to the database session
            db.session.add(user)
        
        # Commit the changes to the database
        db.session.commit()

if __name__ == '__main__':
    # Specify the path to your CSV file
    csv_file_path = 'indices.csv'
    # Call the function to insert data from the CSV file into the database
    insert_data_from_csv(csv_file_path)
