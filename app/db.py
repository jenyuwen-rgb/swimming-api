# app/db.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Render 的 Environment 已經有 DATABASE_URL（含 sslmode=require）
DATABASE_URL = os.environ["DATABASE_URL"]

# 允許用環境變數微調；不設就用預設值
POOL_SIZE     = int(os.getenv("POOL_SIZE", "5"))   # 原本 2 太緊
MAX_OVERFLOW  = int(os.getenv("MAX_OVERFLOW", "5"))
POOL_TIMEOUT  = int(os.getenv("POOL_TIMEOUT", "15"))  # 等待池中連線最久(秒)
POOL_RECYCLE  = int(os.getenv("POOL_RECYCLE", "300")) # 連線回收，避免閒置被砍

engine = create_engine(
    DATABASE_URL,
    pool_size=POOL_SIZE,
    max_overflow=MAX_OVERFLOW,
    pool_timeout=POOL_TIMEOUT,
    pool_recycle=POOL_RECYCLE,
    pool_pre_ping=True,   # 斷線自動重連
    future=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)