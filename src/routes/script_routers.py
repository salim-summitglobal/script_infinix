import os
import tempfile
from datetime import datetime


from fastapi import APIRouter, HTTPException, UploadFile, File

from config.spark_config import spark

router = APIRouter()

@router.post("/generate/audience")
async def get_generate_audience(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        decoded_content = contents.decode("utf-8")

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp:
            tmp.write(decoded_content)
            temp_path = tmp.name


        spark_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(temp_path)


        spark_df.printSchema()
        spark_df.createOrReplaceTempView("temp_table")

        query_select = []
        for col in spark_df.columns:
            if col not in ['education', 'financial_services','media_entertainment']:
                continue
            count = 0
            if col == 'education':
                count = 10
            if col == 'financial_services':
                count = 30
            if col == 'media_entertainment':
                count = 80
            query_select.append(f"{count} AS {col}_probability")

        result_df = spark.sql(f"""
            SELECT *, {', '.join(query_select)}
            FROM temp_table
        """)

        result_df.printSchema()
        folder_number = str(int(datetime.now().timestamp()))
        base_directory = os.path.join(os.path.expanduser("~"), "Downloads")
        output_path = os.path.join(base_directory, f"output_{folder_number}")

        result_df.write.csv(path=output_path, header=True, mode="overwrite")


        return {
            "message": f"File processed successfully Downloads/output_{folder_number}/",
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        os.remove(temp_path)
        spark.stop()