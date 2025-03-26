from transformers import pipeline

# Load a simple text generation model
generator = pipeline("text-generation", model="gpt2")

def query_csv(file_data: dict, query: str) -> str:
    csv_content = file_data["document"]
    # Search for query in any value of the rows
    relevant_rows = [row for row in csv_content if any(query.lower() in str(value).lower() for value in row.values())]
    
    if not relevant_rows:
        return "No relevant data found in the CSV."
    
    context = "\n".join([str(row) for row in relevant_rows])
    prompt = f"Based on this data:\n{context}\nAnswer this: {query}"
    response = generator(prompt, max_length=100, num_return_sequences=1)[0]["generated_text"]
    return response