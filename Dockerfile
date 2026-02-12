FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "etl_pipeline.py", "--input", "data/events.csv", "--out-dir", "output/parquet", "--chunk-size", "20000"]
