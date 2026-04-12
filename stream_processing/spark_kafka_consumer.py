"""
Stream Processing Module - Logistics Real-Time


"""

import os
import json
import math
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, udf, window, avg, count,
    current_timestamp, lit, to_json, struct
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, LongType, TimestampType
)

from map_matching  import MapMatcher
from redis_manager  import RedisWriter

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("StreamProcessor")

# ─── Cấu hình ─────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "gps_stream")
REDIS_HOST              = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT              = int(os.getenv("REDIS_PORT", 6379))
EDGES_JSON              = os.getenv("EDGES_JSON", "../data/edges_schema.json")  # file do map_processor.py tạo ra

# Tần suất cập nhật Redis (giây)
WATERMARK_DELAY   = "10 seconds"   # chấp nhận late data tới 10s
WINDOW_DURATION   = "30 seconds"   # cửa sổ tổng hợp 30s
SLIDE_DURATION    = "10 seconds"   # trượt mỗi 10s

# Ngưỡng phát hiện tắc đường (km/h)
CONGESTION_THRESHOLD_KMH = 5.0

# ─── Schema của message GPS từ Kafka ─────────────────────────────────────────
GPS_SCHEMA = StructType([
    StructField("entity_id",   StringType(),  True),
    StructField("entity_type", StringType(),  True),   # "Truck" | "Bot"
    StructField("latitude",    DoubleType(),  True),
    StructField("longitude",   DoubleType(),  True),
    StructField("speed",       DoubleType(),  True),   # km/h
    StructField("timestamp",   LongType(),    True),   # epoch ms
])


# ─── UDF: Map-matching GPS → edge_id ─────────────────────────────────────────
# Khởi tạo MapMatcher một lần ở driver, broadcast sang executor
_matcher_broadcast = None

def get_map_matcher():
    global _matcher_broadcast
    if _matcher_broadcast is None:
        _matcher_broadcast = MapMatcher(EDGES_JSON)
    return _matcher_broadcast


def _match_to_edge(lat: float, lon: float) -> str:
    """
    Tìm edge gần nhất với tọa độ GPS.
    Trả về edge_id dạng 'E_{u}_{v}' hoặc 'UNKNOWN'.
    """
    if lat is None or lon is None:
        return "UNKNOWN"
    try:
        matcher = get_map_matcher()
        edge_id = matcher.snap_to_edge(lat, lon)
        return edge_id
    except Exception:
        return "UNKNOWN"


match_to_edge_udf = udf(_match_to_edge, StringType())


# ─── Hàm ghi batch vào Redis ──────────────────────────────────────────────────
def write_edge_stats_to_redis(batch_df, batch_id: int):
    """
    foreachBatch callback:
    - Nhận DataFrame đã được tổng hợp theo edge + time window
    - Tính estimated_travel_time
    - Ghi vào Redis với key: edge:{edge_id}
    """
    if batch_df.rdd.isEmpty():
        logger.info(f"[Batch {batch_id}] Rỗng, bỏ qua.")
        return

    logger.info(f"[Batch {batch_id}] Bắt đầu ghi Redis...")

    rows = batch_df.collect()
    writer = RedisWriter(host=REDIS_HOST, port=REDIS_PORT)

    written = 0
    for row in rows:
        edge_id   = row["edge_id"]
        avg_speed = row["avg_speed"]       # km/h
        veh_count = row["vehicle_count"]
        distance  = row["edge_length_m"]   # metres (từ graph)

        if edge_id == "UNKNOWN" or avg_speed is None or avg_speed <= 0:
            continue

        # Tính thời gian di chuyển ước lượng: t = d / v (giây)
        estimated_travel_time = (distance / 1000.0) / avg_speed * 3600.0

        # Gắn nhãn tắc đường
        is_congested = avg_speed < CONGESTION_THRESHOLD_KMH

        payload = {
            "edge_id":                edge_id,
            "avg_speed":              round(avg_speed, 2),
            "vehicle_count":          int(veh_count),
            "distance":               round(distance, 1),
            "estimated_travel_time":  round(estimated_travel_time, 2),
            "is_congested":           is_congested,
            "updated_at":             row["window_end"].isoformat()
                                      if row.get("window_end") else "",
        }

        writer.set_edge_state(edge_id, payload, ttl_seconds=120)
        written += 1

    logger.info(f"[Batch {batch_id}] Đã ghi {written} edges vào Redis.")
    writer.close()


# ─── Hàm bổ sung edge_length từ graph (broadcast) ────────────────────────────
def _get_edge_length(edge_id: str) -> float:
    """Lấy chiều dài edge từ graph (metres). Fallback 500m nếu không tìm thấy."""
    try:
        matcher = get_map_matcher()
        return matcher.get_edge_length(edge_id)
    except Exception:
        return 500.0

get_edge_length_udf = udf(_get_edge_length, DoubleType())


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    logger.info("Khởi động PySpark Structured Streaming...")

    spark = (
        SparkSession.builder
        .appName("LogisticsStreamProcessing")
        # Kafka connector (cần jar trong classpath hoặc --packages)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        )
        # Tắt UI nếu chạy headless
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── 1. Đọc từ Kafka ───────────────────────────────────────────────────────
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── 2. Parse JSON ─────────────────────────────────────────────────────────
    parsed_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), GPS_SCHEMA).alias("data"))
        .select("data.*")
        .filter(col("entity_type").isin("Truck", "Bot"))
        .filter(col("latitude").isNotNull() & col("longitude").isNotNull())
    )

    # Chuyển epoch ms → Timestamp để dùng watermark
    with_ts_df = parsed_df.withColumn(
        "event_time",
        (col("timestamp") / 1000).cast(TimestampType())
    )

    # ── 3. Map-matching GPS → edge_id ─────────────────────────────────────────
    matched_df = with_ts_df.withColumn(
        "edge_id",
        match_to_edge_udf(col("latitude"), col("longitude"))
    )

    # ── 4. Watermark + Window + Aggregate ─────────────────────────────────────
    aggregated_df = (
        matched_df
        .withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(
            window(col("event_time"), WINDOW_DURATION, SLIDE_DURATION),
            col("edge_id")
        )
        .agg(
            avg("speed").alias("avg_speed"),
            count("entity_id").alias("vehicle_count"),
        )
        .select(
            col("edge_id"),
            col("avg_speed"),
            col("vehicle_count"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
        )
    )

    # ── 5. Bổ sung edge_length từ graph ──────────────────────────────────────
    enriched_df = aggregated_df.withColumn(
        "edge_length_m",
        get_edge_length_udf(col("edge_id"))
    )

    # ── 6. Ghi vào Redis qua foreachBatch ─────────────────────────────────────
    query = (
        enriched_df.writeStream
        .outputMode("update")
        .foreachBatch(write_edge_stats_to_redis)
        .option("checkpointLocation", "/tmp/spark_checkpoint/gps_stream")
        .trigger(processingTime="10 seconds")
        .start()
    )

    logger.info("Stream đang chạy. Nhấn Ctrl+C để dừng.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
