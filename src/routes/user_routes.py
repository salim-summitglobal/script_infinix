from fastapi import APIRouter, HTTPException
from typing import List
from pyspark.sql.functions import col

from config.spark_config import spark, df

from src.models.user_models import User, UserResponse, MessageResponse

router = APIRouter()

@router.get("/users", response_model=List[UserResponse])
async def get_users():
    try:
        users = df.collect()
        return [{"name": row.name, "age": row.age} for row in users]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/users/{name}", response_model=UserResponse)
async def get_user(name: str):
    try:
        user = df.filter(col("name") == name).collect()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return {"name": user[0].name, "age": user[0].age}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/users", response_model=MessageResponse)
async def add_user(user: User):
    try:
        global df
        new_data = [(user.name, user.age)]
        new_df = spark.createDataFrame(new_data, df.schema)
        df = df.union(new_df)
        return {"message": "User added successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
