from pymongo import MongoClient

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/")
db = client["rag_csv_db"]
collection = db["csv_files"]

def store_csv(file_id: str, file_name: str, csv_content: list):
    """Store CSV data in MongoDB."""
    document = {
        "file_id": file_id,
        "file_name": file_name,
        "document": csv_content  # Store as list of dicts
    }
    collection.insert_one(document)

def get_file(file_id: str):
    """Retrieve CSV data by file_id."""
    return collection.find_one({"file_id": file_id})

def get_all_files():
    """List all stored files."""
    return list(collection.find({}, {"file_id": 1, "file_name": 1, "_id": 0}))

def delete_file(file_id: str):
    """Delete a file by file_id."""
    return collection.delete_one({"file_id": file_id})