# Databricks notebook source
# MAGIC %pip install neo4j==5.20.0

# COMMAND ----------

EVENT_HUB_CONNECTION_STRING = "YOUR_EVENT_HUB_CONNECTION_STRING_HERE"
NEO4J_URI      = "YOUR_NEO4J_URI_HERE"
NEO4J_PASSWORD = "YOUR_NEO4J_PASSWORD_HERE"
EVENT_HUB_NAME              = "tube-line-status"
CONSUMER_GROUP              = "$Default"

EH_KAFKA_CONFIG = {
    "kafka.bootstrap.servers":                    "evhns-tfl-transit.servicebus.windows.net:9093",
    "kafka.security.protocol":                    "SASL_SSL",
    "kafka.sasl.mechanism":                       "PLAIN",
    "kafka.sasl.jaas.config":                     f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EVENT_HUB_CONNECTION_STRING}";',
    "kafka.request.timeout.ms":                   "60000",
    "kafka.session.timeout.ms":                   "30000",
    "startingOffsets":                            "latest",
    "subscribe":                                  EVENT_HUB_NAME,
    "failOnDataLoss":                             "false",
}

# --- Alert Threshold ---
# If bus capacity within 300m covers less than  70% of displaced commuters
# we raise a Gridlock Alert
GRIDLOCK_THRESHOLD_PERCENT = 70

print("Configuration loaded.")

# COMMAND ----------

# Connect to Event Hubs and read the raw stream

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, cast
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType
)

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

raw_stream = (
    spark.readStream
    .format("kafka")
    .options(**EH_KAFKA_CONFIG)
    .load()
)

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
print(f"Stream schema:")
parsed_stream.printSchema()

# COMMAND ----------

# Filter the stream to disrupted lines only

from pyspark.sql.functions import current_timestamp

disrupted_stream = (
    parsed_stream
    .filter(col("is_disrupted") == True)
    .withColumn("processed_at_utc", current_timestamp())
)

print("Disruption filter applied.")
print("Only lines with is_disrupted=True will be processed downstream.")
print("\nFiltered stream schema:")
disrupted_stream.printSchema()

# COMMAND ----------

from neo4j import GraphDatabase
from pyspark.sql import DataFrame
import json

def get_nearby_bus_options(line_id: str, neo4j_driver) -> list[dict]:
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


def process_disruption_batch(batch_df: DataFrame, batch_id: int):
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

            print(f"\n  Disruption: {line_name} — {severity}")

            bus_options   = get_nearby_bus_options(line_id, driver)
            unique_routes = list(set(b["route_name"] for b in bus_options))
            unique_stops  = list(set(b["station_name"] for b in bus_options))

            gridlock_alert = len(unique_routes) < 3

            print(f"  Affected stations:  {len(unique_stops)}")
            print(f"  Available routes:   {unique_routes[:10]}")

            status = "GRIDLOCK ALERT" if gridlock_alert else "Alternatives available"
            print(f"  {status} for {line_name}")

            # Build structured alert — this shape goes into Delta Lake
            alert = {
                "line_id":            line_id,
                "line_name":          line_name,
                "severity":           severity,
                "gridlock_alert":     gridlock_alert,
                "affected_stations":  unique_stops,
                "available_routes":   unique_routes,
                "nearby_bus_stops":   [
                    {
                        "station_name":    b["station_name"],
                        "bus_stop_name":   b["bus_stop_name"],
                        "bus_stop_id":     b["bus_stop_id"],
                        "lat":             b["lat"],
                        "lon":             b["lon"],
                        "distance_metres": b["distance_metres"],
                        "route_name":      b["route_name"],
                    }
                    for b in bus_options[:20]  # Top 20 closest stops
                ],
                "batch_id":           batch_id,
                "ingested_at_utc":    row["ingested_at_utc"],
                "processed_at_utc":   str(row["processed_at_utc"]),
            }

            alerts.append(alert)

    finally:
        driver.close()

    if alerts:
        alerts_json = json.dumps(alerts)
        print(f"\n  📦 {len(alerts)} alert(s) ready for Delta Lake write.")
        print(f"  Preview: {alerts_json[:200]}...")

        write_alerts_to_delta(alerts, batch_id)

print("Cell 5 defined successfully.")

# COMMAND ----------

# Run this in a NEW cell BEFORE Cell 6
dbutils.fs.rm("file:///tmp/tfl_line_status_checkpoint", recurse=True)
dbutils.fs.rm("file:///tmp/delta/gridlock_alerts", recurse=True)
print("Checkpoint and Delta table cleared.")

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType,
    BooleanType, IntegerType, ArrayType, DoubleType
)
import json

# Schema for each individual nearby bus stop record
bus_stop_schema = StructType([
    StructField("station_name",    StringType(),  True),
    StructField("bus_stop_name",   StringType(),  True),
    StructField("bus_stop_id",     StringType(),  True),
    StructField("lat",             DoubleType(),  True),
    StructField("lon",             DoubleType(),  True),
    StructField("distance_metres", DoubleType(),  True),
    StructField("route_name",      StringType(),  True),
])

# Schema for the top-level Gridlock Alert record
alert_schema = StructType([
    StructField("line_id",           StringType(),              True),
    StructField("line_name",         StringType(),              True),
    StructField("severity",          StringType(),              True),
    StructField("gridlock_alert",    BooleanType(),             True),
    StructField("affected_stations", ArrayType(StringType()),   True),
    StructField("available_routes",  ArrayType(StringType()),   True),
    StructField("nearby_bus_stops",  ArrayType(bus_stop_schema),True),
    StructField("batch_id",          IntegerType(),             True),
    StructField("ingested_at_utc",   StringType(),              True),
    StructField("processed_at_utc",  StringType(),              True),
])

# Delta table storage path
DELTA_TABLE_PATH = "file:///tmp/delta/gridlock_alerts"


def write_alerts_to_delta(alerts: list[dict], batch_id: int):
    """
    Writes a list of Gridlock Alert dicts to a Delta Lake table.

    Uses append mode — each batch adds new rows, preserving full
    alert history. Safe for concurrent reads from Streamlit.

    Args:
        alerts:   List of alert dicts from process_disruption_batch()
        batch_id: Current Spark batch ID for logging
    """
    if not alerts:
        return

    try:
        # Convert list of dicts → Spark DataFrame using our schema
        alerts_df = spark.createDataFrame(alerts, schema=alert_schema)

        # Write to Delta Lake in append mode
        (
            alerts_df
            .write
            .format("delta")
            .mode("append")
            .save(DELTA_TABLE_PATH)
        )

        print(f"  Written {len(alerts)} alert(s) to Delta Lake.")

    except Exception as e:
        print(f"  Delta Lake write failed: {type(e).__name__}: {e}")


CHECKPOINT_PATH = "file:///tmp/tfl_line_status_checkpoint"

print("Starting TfL Disruption Stream Processor...")
print(f"Listening to Event Hub:  {EVENT_HUB_NAME}")
print(f"Writing alerts to:       {DELTA_TABLE_PATH}")
print(f"Checkpoint path:         {CHECKPOINT_PATH}")
print("Waiting for disruption events...\n")

query = (
    disrupted_stream
    .writeStream
    .foreachBatch(process_disruption_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="30 seconds")
    .start()
)

# Monitor for 5 minutes (10 cycles)
import time
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

# Verify Delta table contents
delta_df = spark.read.format("delta").load(DELTA_TABLE_PATH)
delta_df.select("line_name", "severity", "gridlock_alert", "processed_at_utc").show(20, truncate=False)

# COMMAND ----------

delta_df = spark.read.format("delta").load(DELTA_TABLE_PATH)

print(f"Total alerts in Delta table: {delta_df.count()}")
print(f"Unique lines: {[row[0] for row in delta_df.select('line_name').distinct().collect()]}")
print(f"Date range: {delta_df.selectExpr('min(processed_at_utc)', 'max(processed_at_utc)').collect()}")

delta_df.select(
    "line_name",
    "severity", 
    "gridlock_alert",
    "processed_at_utc"
).orderBy("processed_at_utc", ascending=False).show(10, truncate=False)