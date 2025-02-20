import csv
import io
import tempfile

from fastapi import APIRouter, HTTPException, UploadFile, File

from config.spark_config import spark

router = APIRouter()

@router.post("/generate/audience")
async def get_generate_audience(file: UploadFile = File(...)):
    """
    Endpoint untuk meng-upload file CSV, lalu dibaca dengan Spark.
    """
    try:
        # 1. Baca isi file yang di-upload ke dalam memori
        contents = await file.read()
        decoded_content = contents.decode("utf-8")

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp:
            tmp.write(decoded_content)
            temp_path = tmp.name  # path ke file CSV sementara


        spark_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(temp_path)


        print("DataFrame Schema:")
        spark_df.printSchema()

        rows = spark_df.collect()
        data_list = [row.asDict() for row in rows]

        return {
            "message": "File processed successfully",
            "data": data_list
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))