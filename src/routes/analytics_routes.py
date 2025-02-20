from fastapi import APIRouter, HTTPException
from config.spark_config import df

router = APIRouter()

@router.get("/analytics/age")
async def get_age_analytics():
    try:
        stats = df.select("age").describe().collect()
        return {
            "count": float(stats[0]["age"]),  # row 0 -> count
            "mean": float(stats[1]["age"]),   # row 1 -> mean
            "std": float(stats[2]["age"]),    # row 2 -> stddev
            "min": float(stats[3]["age"]),    # row 3 -> min
            "max": float(stats[4]["age"])     # row 4 -> max
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
