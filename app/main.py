from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes import router as api_router

app = FastAPI(title="swim-api", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 如需上線鎖網域再收斂
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 路由統一掛在 /api（render.yaml 健康檢查也是 /api/health）
app.include_router(api_router, prefix="/api")

@app.get("/")
def root():
    return {"ok": True, "service": "swim-api"}
