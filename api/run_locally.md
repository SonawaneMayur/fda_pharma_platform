# run locally 

export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
export FDA_RAG_API_KEY="dev-key-123"
cd api && uvicorn app:app --reload

# Test it
curl -X POST http://localhost:8000/v1/ask \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-key-123" \
  -d '{"question": "What are adverse events for warfarin?", "user_id": "demo"}'