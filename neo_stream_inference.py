import os
import joblib
import pandas as pd
import boto3

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType
)

# =========================================================
# ENVIRONMENT CONFIGURATION
# =========================================================

MODEL_PATH = os.getenv("MODEL_PATH")
SCALER_PATH = os.getenv("SCALER_PATH")
S3_BUCKET = os.getenv("S3_BUCKET")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "test")

S3_PREFIX = "neo_predictions/"

if not MODEL_PATH or not SCALER_PATH:
    raise ValueError("MODEL_PATH or SCALER_PATH not set in environment variables")

if not S3_BUCKET:
    raise ValueError("S3_BUCKET not set in environment variables")

# =========================================================
# LOAD MODEL & SCALER
# =========================================================

print("🔁 Loading ML model and scaler...")
model = joblib.load(MODEL_PATH)
scaler = joblib.load(SCALER_PATH)
print("✅ Model and scaler loaded successfully.")

# =========================================================
# AWS S3 CLIENT
# =========================================================

s3 = boto3.client("s3")

# =========================================================
# SPARK SESSION
# =========================================================

spark = SparkSession.builder \
    .appName("NASA-NEO-Live-Inference-To-S3") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =========================================================
# DEFINE NASA NEO SCHEMA
# =========================================================

neo_schema = StructType([
    StructField("id", StringType()),
    StructField("absolute_magnitude_h", DoubleType()),
    StructField("estimated_diameter", StructType([
        StructField("kilometers", StructType([
            StructField("estimated_diameter_max", DoubleType())
        ]))
    ])),
    StructField("close_approach_data", ArrayType(
        StructType([
            StructField("relative_velocity", StructType([
                StructField("kilometers_per_second", StringType())
            ])),
            StructField("miss_distance", StructType([
                StructField("kilometers", StringType()),
                StructField("lunar", StringType())
            ]))
        ])
    ))
])

# =========================================================
# READ FROM KAFKA STREAM
# =========================================================

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

parsed = raw_stream.select(
    from_json(col("value").cast("string"), neo_schema).alias("data")
)

# =========================================================
# FEATURE EXTRACTION
# =========================================================

df = parsed.select(
    col("data.id").alias("asteroid_id"),
    col("data.absolute_magnitude_h"),
    col("data.estimated_diameter.kilometers.estimated_diameter_max")
        .alias("diameter_max_km"),
    explode(col("data.close_approach_data")).alias("cad")
)

df = df.select(
    "asteroid_id",
    "absolute_magnitude_h",
    "diameter_max_km",
    col("cad.relative_velocity.kilometers_per_second")
        .cast("double").alias("relative_velocity_km_s"),
    col("cad.miss_distance.kilometers")
        .cast("double").alias("miss_distance_km"),
    col("cad.miss_distance.lunar")
        .cast("double").alias("miss_distance_lunar")
)

# =========================================================
# FEATURE ENGINEERING
# =========================================================

df = df.withColumn(
    "impact_energy_proxy",
    col("diameter_max_km") * (col("relative_velocity_km_s") ** 2)
)

df = df.withColumn(
    "risk_score",
    col("impact_energy_proxy") / col("miss_distance_km")
)

df = df.withColumn("ingestion_time", current_timestamp())

# =========================================================
# STREAMING INFERENCE + S3 STORAGE
# =========================================================

def predict_and_store(batch_df, batch_id):

    if batch_df.rdd.isEmpty():
        return

    print(f"\n🚀 Processing batch {batch_id}...")

    pdf = batch_df.toPandas()

    FEATURES = [
        "absolute_magnitude_h",
        "miss_distance_lunar",
        "diameter_max_km",
        "relative_velocity_km_s",
        "miss_distance_km",
        "impact_energy_proxy",
        "risk_score"
    ]

    # Scale features
    X_scaled = scaler.transform(pdf[FEATURES])

    # Predict
    pdf["prediction"] = model.predict(X_scaled)
    pdf["hazard_probability"] = model.predict_proba(X_scaled)[:, 1]

    # Convert to JSON
    json_data = pdf.to_json(orient="records", date_format="iso")

    # S3 Key
    key = (
        f"{S3_PREFIX}"
        f"date={pd.Timestamp.now('UTC').date()}/"
        f"batch_{batch_id}.json"
    )

    # Upload to S3
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json_data
    )

    print(f"📤 Uploaded batch to S3 → {key}")

    # Print predictions
    for _, row in pdf.iterrows():
        print(
            f"🛰️ Asteroid {row['asteroid_id']} | "
            f"Prediction: {row['prediction']} | "
            f"Hazard Prob: {row['hazard_probability']:.4f}"
        )

# =========================================================
# START STREAM
# =========================================================

query = df.writeStream \
    .foreachBatch(predict_and_store) \
    .option("checkpointLocation", "spark_checkpoints/nasa_neo_s3") \
    .outputMode("append") \
    .start()

print("🟢 Streaming started... Waiting for data.")

query.awaitTermination()