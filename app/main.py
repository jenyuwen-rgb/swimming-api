# app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes import router

app = FastAPI(title="Swimming API")

# CORS（可視需要調整）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 統一掛上 /api 前綴
app.include_router(router, prefix="/api")

@app.get("/")
def root():
    return {"ok": True, "service": "swimming-api"}
