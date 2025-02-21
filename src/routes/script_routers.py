import io
import os
import tempfile
from datetime import datetime


from fastapi import APIRouter, HTTPException, UploadFile, File, Depends

from config.spark_config import spark
from src.s3.aws_s3 import S3Helper

router = APIRouter()

@router.post("/generate/audience")
async def get_generate_audience(file: UploadFile = File(...),s3_helper: S3Helper = Depends()):
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
        unique_filename = f"output/output_{folder_number}.csv"


        pdf = result_df.toPandas()
        csv_string = pdf.to_csv(index=False)
        binary_csv = io.BytesIO(csv_string.encode("utf-8"))

        await s3_helper.upload_file(
            file_content=binary_csv,
            file_name=unique_filename,
            content_type="text/csv"
        )


        return {
            "message": f"File processed successfully and uploaded to S3 as {unique_filename}"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        os.remove(temp_path)
        spark.stop()