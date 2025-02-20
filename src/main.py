from fastapi import FastAPI
from uvicorn import run


from src.routes.script_routers import router as script_router



app = FastAPI(
    title="Spark FastAPI",
    description="API for Apache Spark operations",
    version="1.0.0"
)

app.include_router(script_router, prefix="")



if __name__ == "__main__":
    run("src.main:app", host="0.0.0.0", port=8002, reload=True)
