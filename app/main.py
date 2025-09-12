# app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes import router

app = FastAPI(title="swim-api")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ 統一由這裡加上 /api 前綴
app.include_router(router, prefix="/api")


@app.get("/")
def root():
    return {"message": "swim-api OK"}
