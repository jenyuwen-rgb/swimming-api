from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import router
import os
from dotenv import load_dotenv

# 載入環境變數
load_dotenv()
API_ORIGIN = os.getenv("API_ORIGIN", "")

app = FastAPI()

# CORS 設定
app.add_middleware(
    CORSMiddleware,
    allow_origins=[API_ORIGIN] if API_ORIGIN else ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 基本測試路由
@app.get("/")
def index():
    return {"ok": True, "service": "swimming-record"}

@app.get("/health")
def health():
    return {"status": "healthy"}

# 其他 API 路由
app.include_router(router, prefix="/api")
