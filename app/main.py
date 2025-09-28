# app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
from sqlalchemy.exc import OperationalError

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

# ✅ DB 連線錯誤 → 回 503，前端可重試
@app.exception_handler(OperationalError)
def db_op_err_handler(_req, exc: OperationalError):
    return JSONResponse(
        status_code=503,
        content={
            "detail": "database temporarily unavailable",
            "error": "OperationalError",
        },
    )

@app.get("/")
def root():
    return {"message": "swim-api OK"}