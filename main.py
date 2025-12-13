from fastapi import FastAPI

app = FastAPI(title="ProbLabs API")

@app.get("/health")
def health():
    return {"status": "ok", "service": "problabs-backend"}
