# Databricks notebook source
# MAGIC %pip install neo4j==5.20.0

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, BooleanType, DoubleType,
    ArrayType
)
from neo4j import GraphDatabase
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType,
    BooleanType, IntegerType, ArrayType, DoubleType
)
import json
import time

# COMMAND ----------

EVENT_HUB_CONNECTION_STRING = dbutils.secrets.get(
    scope="tfl-transit-scope",
    key="event-hub-connection-string"
)
NEO4J_PASSWORD = dbutils.secrets.get(
    scope="tfl-transit-scope",
    key="neo4j-password"
)
ADLS_ACCESS_KEY = dbutils.secrets.get(
    scope="tfl-transit-scope",
    key="adls-access-key"
)

EVENT_HUB_NAME      = "tube-line-status"
CONSUMER_GROUP      = "$Default"
NEO4J_URI           = "neo4j+s://48be38f7.databases.neo4j.io"
NEO4J_USERNAME      = "48be38f7"
ADLS_ACCOUNT_NAME   = "datalaketfltransit"
ADLS_CONTAINER_NAME = "transit-alerts"

# --- Event Hubs Kafka config ---
EH_KAFKA_CONFIG = {
    "kafka.bootstrap.servers":  "evhns-tfl-transit.servicebus.windows.net:9093",
    "kafka.security.protocol":  "SASL_SSL",
    "kafka.sasl.mechanism":     "PLAIN",
    "kafka.sasl.jaas.config":   f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EVENT_HUB_CONNECTION_STRING}";',
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "30000",
    "startingOffsets":          "latest",
    "subscribe":                EVENT_HUB_NAME,
    "failOnDataLoss":           "false",
}

# --- Configure Spark for ADLS Gen2 ---
spark.conf.set(
    f"fs.azure.account.key.{ADLS_ACCOUNT_NAME}.dfs.core.windows.net",
    ADLS_ACCESS_KEY
)

# --- Paths ---
ADLS_BASE_PATH   = f"abfss://{ADLS_CONTAINER_NAME}@{ADLS_ACCOUNT_NAME}.dfs.core.windows.net"
DELTA_TABLE_PATH = f"{ADLS_BASE_PATH}/gridlock_alerts"
CHECKPOINT_PATH  = f"{ADLS_BASE_PATH}/checkpoints/gridlock_alerts"

GRIDLOCK_THRESHOLD_PERCENT = 70

print("Configuration loaded from Databricks Secrets.")
print(f"Delta table path: {DELTA_TABLE_PATH}")
print(f"Checkpoint path:  {CHECKPOINT_PATH}")

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

event_schema = StructType([
    StructField("event_type",        StringType(),  True),
    StructField("line_id",           StringType(),  True),
    StructField("line_name",         StringType(),  True),
    StructField("severity_code",     IntegerType(), True),
    StructField("severity_desc",     StringType(),  True),
    StructField("is_disrupted",      BooleanType(), True),
    StructField("disruption_detail", StringType(),  True),
    StructField("ingested_at_utc",   StringType(),  True),
])

# Read raw stream from Event Hubs via Kafka API
raw_stream = (
    spark.readStream
    .format("kafka")
    .options(**EH_KAFKA_CONFIG)
    .load()
)

# Parse JSON payload
parsed_stream = (
    raw_stream
    .select(
        from_json(
            col("value").cast("string"),
            event_schema
        ).alias("data"),
        col("timestamp").alias("event_hub_timestamp")
    )
    .select("data.*", "event_hub_timestamp")
)

print("Stream defined successfully.")
parsed_stream.printSchema()

# COMMAND ----------

# Filter the stream to disrupted lines only
disrupted_stream = (
    parsed_stream
    .filter(col("is_disrupted") == True)
    .withColumn("processed_at_utc", current_timestamp())
)

disrupted_stream.printSchema()

# COMMAND ----------

# Delta Lake Schema
bus_stop_schema = StructType([
    StructField("station_name",    StringType(), True),
    StructField("bus_stop_name",   StringType(), True),
    StructField("bus_stop_id",     StringType(), True),
    StructField("lat",             DoubleType(), True),
    StructField("lon",             DoubleType(), True),
    StructField("distance_metres", DoubleType(), True),
    StructField("route_name",      StringType(), True),
])

alert_schema = StructType([
    StructField("line_id",           StringType(),               True),
    StructField("line_name",         StringType(),               True),
    StructField("severity",          StringType(),               True),
    StructField("gridlock_alert",    BooleanType(),              True),
    StructField("affected_stations", ArrayType(StringType()),    True),
    StructField("available_routes",  ArrayType(StringType()),    True),
    StructField("nearby_bus_stops",  ArrayType(bus_stop_schema), True),
    StructField("batch_id",          IntegerType(),              True),
    StructField("ingested_at_utc",   StringType(),               True),
    StructField("processed_at_utc",  StringType(),               True),
])

def get_nearby_bus_options(line_id: str, neo4j_driver) -> list[dict]:
    """
    Queries Neo4j for bus stops near stations on the disrupted line.
    Uses TubeLine->SERVES->TubeStation traversal for precision.
    """
    cypher = """
        MATCH (l:TubeLine)-[:SERVES]->(s:TubeStation)-[r:HAS_NEARBY_STOP]->(b:BusStop)-[:SERVED_BY]->(route:BusRoute)
        WHERE l.line_id = $line_id
        RETURN s.name             AS station_name,
               b.name            AS bus_stop_name,
               b.stop_id         AS bus_stop_id,
               b.lat             AS lat,
               b.lon             AS lon,
               r.distance_metres AS distance_metres,
               route.name        AS route_name
        ORDER BY r.distance_metres ASC
        LIMIT 100
    """
    with neo4j_driver.session() as session:
        result = session.run(cypher, {"line_id": line_id})
        return [record.data() for record in result]

def write_alerts_to_delta(alerts: list[dict], batch_id: int):
    """
    Writes Gridlock Alert dicts to Delta Lake on ADLS Gen2.
    Uses append mode — preserves full alert history.
    """
    if not alerts:
        return
    try:
        alerts_df = spark.createDataFrame(alerts, schema=alert_schema)
        (
            alerts_df
            .write
            .format("delta")
            .mode("append")
            .save(DELTA_TABLE_PATH)
        )
        print(f"Written {len(alerts)} alert(s) to Delta Lake.")
    except Exception as e:
        print(f"Delta Lake write failed: {type(e).__name__}: {e}")

def process_disruption_batch(batch_df: DataFrame, batch_id: int):
    """
    Processes each micro-batch of disrupted line events.
    Queries Neo4j and writes Gridlock Alerts to Delta Lake.
    """
    if batch_df.count() == 0:
        print(f"Batch {batch_id}: No disruptions detected.")
        return

    print(f"\nBatch {batch_id}: Processing {batch_df.count()} disrupted line(s)...")

    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
    )

    alerts = []

    try:
        disruptions = batch_df.collect()

        for row in disruptions:
            line_id   = row["line_id"]
            line_name = row["line_name"]
            severity  = row["severity_desc"]

            print(f"\nDisruption: {line_name} — {severity}")

            bus_options   = get_nearby_bus_options(line_id, driver)
            unique_routes = list(set(b["route_name"] for b in bus_options))
            unique_stops  = list(set(b["station_name"] for b in bus_options))

            gridlock_alert = len(unique_routes) < 3

            print(f"Affected stations: {len(unique_stops)}")
            print(f"Available routes:  {unique_routes[:10]}")

            status = "GRIDLOCK ALERT" if gridlock_alert else "Alternatives available"
            print(f"  {status} for {line_name}")

            alert = {
                "line_id":           line_id,
                "line_name":         line_name,
                "severity":          severity,
                "gridlock_alert":    gridlock_alert,
                "affected_stations": unique_stops,
                "available_routes":  unique_routes,
                "nearby_bus_stops":  [
                    {
                        "station_name":    b["station_name"],
                        "bus_stop_name":   b["bus_stop_name"],
                        "bus_stop_id":     b["bus_stop_id"],
                        "lat":             b["lat"],
                        "lon":             b["lon"],
                        "distance_metres": b["distance_metres"],
                        "route_name":      b["route_name"],
                    }
                    for b in bus_options[:20]
                ],
                "batch_id":          batch_id,
                "ingested_at_utc":   row["ingested_at_utc"],
                "processed_at_utc":  str(row["processed_at_utc"]),
            }
            alerts.append(alert)

    finally:
        driver.close()

    if alerts:
        print(f"\n{len(alerts)} alert(s) ready for Delta Lake write.")
        write_alerts_to_delta(alerts, batch_id)

# COMMAND ----------

print("Starting TfL Disruption Stream Processor...")
print(f"Listening to Event Hub: {EVENT_HUB_NAME}")
print(f"Writing alerts to:      {DELTA_TABLE_PATH}")
print(f"Checkpoint path:        {CHECKPOINT_PATH}")
print("Waiting for disruption events...\n")

query = (
    disrupted_stream
    .writeStream
    .foreachBatch(process_disruption_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="30 seconds")
    .start()
)

# Monitor for 5 minutes
for i in range(10):
    time.sleep(30)
    progress = query.lastProgress
    if progress:
        print(f"[{i+1}/10] Batch {progress['batchId']} | "
              f"Input rows: {progress['numInputRows']} | "
              f"Processing time: {progress['durationMs'].get('triggerExecution', 0)}ms")
    else:
        print(f"[{i+1}/10] Waiting for first batch...")

# COMMAND ----------

delta_df = spark.read.format("delta").load(DELTA_TABLE_PATH)

print(f"Total alerts: {delta_df.count()}")
print(f"Unique lines: {[r[0] for r in delta_df.select('line_name').distinct().collect()]}")

delta_df.select(
    "line_name",
    "severity",
    "gridlock_alert",
    "processed_at_utc"
).orderBy("processed_at_utc", ascending=False).show(10, truncate=False)