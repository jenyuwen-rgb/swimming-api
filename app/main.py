
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes import router
import os
from dotenv import load_dotenv

load_dotenv()
API_ORIGIN = os.getenv("API_ORIGIN", "")

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=[API_ORIGIN] if API_ORIGIN else ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api")
