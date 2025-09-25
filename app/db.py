# app/db.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,   # 明確使用 QueuePool
    pool_size=2,          # 小一點，避免過多併發佔用連線池
    max_overflow=0,       # 不溢出，穩定一點
    pool_timeout=30,      # 等連線最多 30s
    pool_recycle=300,     # 300s 回收，避免閒置被遠端關閉
    pool_pre_ping=True,   # 取用前測試連線，壞的自動重連
    connect_args={"sslmode": "require"},  # 強制 SSL（與 Supabase pooler 一致）
    future=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)