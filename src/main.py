# script_infinix/src/main.py

import csv
import io
from fastapi import FastAPI, HTTPException, UploadFile, File
from uvicorn import run

# Import router
from src.routes.user_routes import router as user_router
from src.routes.analytics_routes import router as analytics_router
from src.routes.script_routers import router as script_router

# Import Spark untuk keperluan di endpoint lain (opsional)
from config.spark_config import spark

app = FastAPI(
    title="Spark FastAPI",
    description="API for Apache Spark operations",
    version="1.0.0"
)

# 1. Daftarkan Router
app.include_router(user_router, prefix="")
app.include_router(analytics_router, prefix="")
app.include_router(script_router, prefix="")



if __name__ == "__main__":
    run("src.main:app", host="0.0.0.0", port=8002, reload=True)
