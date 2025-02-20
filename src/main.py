from fastapi import FastAPI, HTTPException
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from typing import List, Optional
from pydantic import BaseModel
from uvicorn import run

# Pydantic models for request/response
class User(BaseModel):
    name: str
    age: int

class UserResponse(BaseModel):
    name: str
    age: int

class MessageResponse(BaseModel):
    message: str

# Initialize Spark
spark = SparkSession.builder \
    .appName("SparkFastAPI") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

# Define schema for Spark DataFrame
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Initial data
initial_data = [("John", 30), ("Alice", 25), ("Bob", 35)]
df = spark.createDataFrame(initial_data, schema)

# Initialize FastAPI
app = FastAPI(title="Spark FastAPI",
              description="API for Apache Spark operations",
              version="1.0.0")

@app.get("/health")
async def health_check():
    return {"status": "healthy", "spark_version": spark.version}

@app.get("/users", response_model=List[UserResponse])
async def get_users():
    try:
        users = df.collect()
        return [{"name": row.name, "age": row.age} for row in users]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/users/{name}", response_model=UserResponse)
async def get_user(name: str):
    try:
        user = df.filter(col("name") == name).collect()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return {"name": user[0].name, "age": user[0].age}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/users", response_model=MessageResponse)
async def add_user(user: User):
    try:
        global df
        new_data = [(user.name, user.age)]
        new_df = spark.createDataFrame(new_data, schema)
        df = df.union(new_df)
        return {"message": "User added successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Add data analysis endpoint
@app.get("/analytics/age")
async def get_age_analytics():
    try:
        stats = df.select("age").describe().collect()
        return {
            "count": float(stats[0]["age"]),
            "mean": float(stats[1]["age"]),
            "std": float(stats[2]["age"]),
            "min": float(stats[3]["age"]),
            "max": float(stats[4]["age"])
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/analytics/age")
async def get_age_analytics():
    try:
        stats = df.select("age").describe().collect()
        return {
            "count": float(stats[0]["age"]),
            "mean": float(stats[1]["age"]),
            "std": float(stats[2]["age"]),
            "min": float(stats[3]["age"]),
            "max": float(stats[4]["age"])
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    run("app:app", host="0.0.0.0", port=8002, reload=True)