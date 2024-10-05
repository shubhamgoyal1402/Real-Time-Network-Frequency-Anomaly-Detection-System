import pandas as pd
from pymongo import MongoClient
from sklearn.ensemble import IsolationForest
import joblib

# Function to fetch data from MongoDB
def fetch_data_from_mongodb():
    print("Fetching data from MongoDB...")
    
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client.network_data
    collection = db.frequency

    # Fetch the data
    data = list(collection.find({}, {"_id": 0, "frequency": 1, "timestamp": 1}))

    # Convert to a pandas DataFrame
    df = pd.DataFrame(data)

    # Save as CSV for inspection (optional)
    df.to_csv('frequency_data.csv', index=False)

    return df

# Function to train the Isolation Forest model
def train_model(df):
    print("Training the Isolation Forest model...")
    
    # Prepare the data (we'll only use the 'frequency' column)
    X = df[['frequency']]

    # Train the Isolation Forest model
    model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
    model.fit(X)

    # Save the model to a file
    joblib.dump(model, 'isolation_forest_model.pkl')

    print("Model trained and saved as isolation_forest_model.pkl")

# Automate data fetching and model training
df = fetch_data_from_mongodb()
train_model(df)
