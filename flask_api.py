import pandas as pd
from pymongo import MongoClient
from sklearn.ensemble import IsolationForest
import joblib
from flask import Flask, request, jsonify
import os
import threading
import time

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

# Function to automate data fetching and model training
def automated_training():
    # Fetch the latest data from MongoDB
    df = fetch_data_from_mongodb()

    # Train the model
    train_model(df)

# Flask App for serving the model
app = Flask(__name__)

# Load the trained model
def load_model():
    print("Loading the Isolation Forest model...")
    if os.path.exists('isolation_forest_model.pkl'):
        return joblib.load('isolation_forest_model.pkl')
    else:
        raise FileNotFoundError("Trained model not found! Please run the training first.")

model = load_model()

@app.route('/predict', methods=['POST'])
def predict():
    # Get frequency data from the request
    data = request.json
    frequency = data.get('frequency', None)

    if frequency is None:
        return jsonify({'error': 'No frequency data provided'}), 400

    # Prepare the data for prediction
    df = pd.DataFrame([[frequency]], columns=['frequency'])

    # Make prediction (1 = normal, -1 = anomaly)
    prediction = model.predict(df)[0]

    # Return the result
    if prediction == -1:
        return jsonify({'anomaly': True, 'frequency': frequency})
    else:
        return jsonify({'anomaly': False, 'frequency': frequency})

# Function to handle periodic model retraining in the background
def periodic_retraining(interval=86400):
    while True:
        print(f"Waiting {interval / 3600} hours before retraining the model...")
        time.sleep(interval)
        automated_training()

if __name__ == '__main__':
    # Start the initial training before the Flask server starts
    print("Starting automated data fetching and model training...")
    automated_training()

    # Start the background thread for periodic retraining
    retraining_thread = threading.Thread(target=periodic_retraining, daemon=True)
    retraining_thread.start()

    # Run the Flask server to serve the predictions
    app.run(debug=True, port=5000)
