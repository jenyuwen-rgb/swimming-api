# main.py
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from app.routes import router  # 依你的實際匯入路徑

app = FastAPI()

origins = [
    "http://localhost:3000",
    "https://swim-web.vercel.app",   # 例如 https://swim-web.vercel.app
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/health")
def health():
    return {"ok": True}

app.include_router(router)


from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes import router as api_router

app = FastAPI(title="Swimming API", version="1.0.0")

# CORS（前端在 Vercel，允許）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://swim-web.vercel.app"],  # 如需更嚴謹可改成你的網域 # 例如 https://swim-web.vercel.app
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 掛載 /api/*
app.include_router(api_router, prefix="/api")

@app.get("/")
def root():
    return {"ok": True, "service": "swimming-api"}
