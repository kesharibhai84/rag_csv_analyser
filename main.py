from fastapi import FastAPI, UploadFile, File, HTTPException
from typing import Optional
import pandas as pd
import uuid
from database import store_csv, get_file, get_all_files, delete_file
from rag import query_csv
from pydantic import BaseModel

class QueryRequest(BaseModel):
    file_id: str
    query: str

app = FastAPI(title="RAG CSV Analyser")

# POST /upload - Upload a CSV file
@app.post("/upload")
async def upload_file(file: UploadFile = File(None), file_path: Optional[str] = None):
    if not file and not file_path:
        raise HTTPException(status_code=400, detail="Provide a file or file path.")
    
    try:
        if file:
            # Handle direct file upload
            df = pd.read_csv(file.file)
            file_name = file.filename
        elif file_path:
            # Handle file from path
            df = pd.read_csv(file_path)
            file_name = file_path.split("/")[-1]
        
        # Convert CSV to list of dicts
        csv_content = df.to_dict(orient="records")
        file_id = str(uuid.uuid4())  # Generate unique ID
        
        # Store in MongoDB
        store_csv(file_id, file_name, csv_content)
        return {"file_id": file_id, "message": "Upload successful"}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

# GET /files - List all files
@app.get("/files")
async def list_files():
    try:
        files = get_all_files()
        return {"files": files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving files: {str(e)}")

# POST /query - Query CSV data
@app.post("/query")
async def query_file(request: QueryRequest):
    if not request.file_id or not request.query:
        raise HTTPException(status_code=400, detail="file_id and query are required.")
    
    file_data = get_file(request.file_id)
    if not file_data:
        raise HTTPException(status_code=404, detail="File not found.")
    
    response = query_csv(file_data, request.query)
    return {"response": response}

# DELETE /file/{file_id} - Delete a file
@app.delete("/file/{file_id}")
async def delete_file_endpoint(file_id: str):
    result = delete_file(file_id)
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="File not found.")
    return {"message": "File deleted successfully"}

# Run the app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)