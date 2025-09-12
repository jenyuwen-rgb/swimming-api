from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes import router as api_router

app = FastAPI(title="swim-api", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 只在這裡掛一次 /api
app.include_router(api_router, prefix="/api")

@app.get("/")
def root():
    return {"message": "Swimming API. See /docs"}
