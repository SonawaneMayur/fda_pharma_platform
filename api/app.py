import os, time, logging, uuid
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import mlflow.deployments

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger("fda-rag-api")

app = FastAPI(title="FDA Pharmacovigilance API", version="1.0.0")

allowed_origins = os.environ.get("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(CORSMiddleware, allow_origins=allowed_origins,
                   allow_methods=["*"], allow_headers=["*"])

client = mlflow.deployments.get_deploy_client("databricks")
ENDPOINT = os.environ.get("DATABRICKS_ENDPOINT", "fda-rag-endpoint")
API_KEY = os.environ["FDA_RAG_API_KEY"]

class Query(BaseModel):
    question: str = Field(min_length=3, max_length=1000)
    user_id: str | None = None

class Answer(BaseModel):
    request_id: str
    answer: str
    latency_ms: int
    model_version: str

@app.middleware("http")
async def add_request_id(request: Request, call_next):
    request.state.request_id = str(uuid.uuid4())
    response = await call_next(request)
    response.headers["X-Request-ID"] = request.state.request_id
    return response

@app.post("/v1/ask", response_model=Answer)
def ask(q: Query, request: Request, x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(401, "invalid api key")

    rid = request.state.request_id
    t0 = time.time()
    try:
        resp = client.predict(endpoint=ENDPOINT, inputs={"question": q.question})
        latency = int((time.time() - t0) * 1000)
        log.info(f"rid={rid} user={q.user_id} latency_ms={latency} q_len={len(q.question)}")
        return Answer(
            request_id=rid,
            answer=resp["predictions"][0] if "predictions" in resp else str(resp),
            latency_ms=latency,
            model_version="1")
    except Exception as e:
        log.exception(f"rid={rid} inference_failed")
        raise HTTPException(500, f"inference error: {str(e)[:200]}")

@app.get("/health")
def health():
    return {"status": "ok", "endpoint": ENDPOINT}
