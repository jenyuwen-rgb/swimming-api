import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# 讀環境變數（例：postgresql://...:6543/postgres?sslmode=require）
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Missing env: DATABASE_URL")

# URL 已帶 sslmode=require，因此不需再在 connect_args 指定
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=5,
    pool_recycle=1800,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
