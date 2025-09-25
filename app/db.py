# app/db.py
import os
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

def _ensure_sslmode(url: str) -> str:
    """
    若連線字串沒有帶 sslmode，幫你補上 ?sslmode=require
    """
    parsed = urlparse(url)
    q = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if "sslmode" not in q:
        q["sslmode"] = "require"
    new_query = urlencode(q)
    return urlunparse(parsed._replace(query=new_query))

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

DATABASE_URL = _ensure_sslmode(DATABASE_URL)

# 判斷是否為 Supabase Pooler（pgBouncer）：host 含 pooler.supabase.com 或 port 6543
p = urlparse(DATABASE_URL)
is_pooler = ("pooler.supabase.com" in (p.hostname or "")) or (p.port == 6543)

# 統一的 connect_args（確保 SSL）
connect_args = {"sslmode": "require"}

if is_pooler:
    # 交由 PgBouncer 管理連線池：本地端禁用 SQLAlchemy 連線池
    engine = create_engine(
        DATABASE_URL,
        poolclass=NullPool,
        pool_pre_ping=True,     # 連線前 ping，避免死連線
        future=True,
        connect_args=connect_args,
    )
else:
    # 直連資料庫：開小池，避免免費方案/小實例被塞爆
    engine = create_engine(
        DATABASE_URL,
        pool_size=2,
        max_overflow=0,
        pool_recycle=300,       # 避免閒置被砍後變成死連線
        pool_pre_ping=True,
        future=True,
        connect_args=connect_args,
    )

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)