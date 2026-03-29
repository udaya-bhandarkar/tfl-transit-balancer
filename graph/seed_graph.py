"""
seed_graph.py

Responsibility: One-time script to populate Neo4j with the TfL network graph.
Run this ONCE to build the graph. It is idempotent — safe to re-run without
creating duplicate nodes (uses MERGE, not CREATE).

What it builds:
    (:TubeStation) -[:HAS_NEARBY_STOP {distance_metres}]-> (:BusStop)
    (:BusStop)     -[:SERVED_BY]->                          (:BusRoute)
"""

import asyncio
import aiohttp
import logging
import os
from dotenv import load_dotenv
from graph.neo4j_connection import Neo4jConnection

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TFL_BASE_URL            = "https://api.tfl.gov.uk"
TFL_APP_KEY             = os.getenv("TFL_APP_KEY")
NEARBY_STOP_RADIUS_M    = 300   # Search radius around each station (metres)
MAX_CONCURRENT_REQUESTS = 5     # Throttle concurrent TfL API calls

# ---------------------------------------------------------------------------
# TfL API Calls
# ---------------------------------------------------------------------------

async def fetch_tube_stations(session: aiohttp.ClientSession) -> list[dict]:
    """
    Fetches all Tube stations from TfL StopPoint API.
    Filters to NaptanMetroStation type only — excludes platforms,
    entrances, and interchange hubs to get one node per station.
    """
    url    = f"{TFL_BASE_URL}/StopPoint/Mode/tube"
    params = {"app_key": TFL_APP_KEY}

    try:
        async with session.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(total=60)
        ) as resp:
            resp.raise_for_status()
            data     = await resp.json()
            stations = data.get("stopPoints", [])

            # Filter to top-level stations only — excludes platforms,
            # entrances, and hub interchange nodes
            stations = [
                s for s in stations
                if s.get("stopType") == "NaptanMetroStation"
            ]

            logger.info(f"Fetched {len(stations)} Tube stations from TfL.")
            return stations

    except Exception as e:
        logger.error(f"Failed to fetch Tube stations: {type(e).__name__}: {e}")
        return []


async def fetch_nearby_bus_stops(
    session:    aiohttp.ClientSession,
    station_id: str,
    lat:        float,
    lon:        float,
    semaphore:  asyncio.Semaphore
) -> list[dict]:
    """
    Fetches bus stops within NEARBY_STOP_RADIUS_M metres of a given station.
    Uses a semaphore to limit concurrent requests and respect TfL rate limits.
    """
    url    = f"{TFL_BASE_URL}/StopPoint"
    params = {
        "lat":       lat,
        "lon":       lon,
        "stopTypes": "NaptanPublicBusCoachTram",
        "radius":    NEARBY_STOP_RADIUS_M,
        "app_key":   TFL_APP_KEY
    }

    async with semaphore:
        await asyncio.sleep(0.5)  # 500ms delay to respect rate limits
        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                resp.raise_for_status()
                data  = await resp.json()
                stops = data.get("stopPoints", [])
                logger.info(
                    f"  Station {station_id}: found {len(stops)} nearby bus stops."
                )
                return stops
        except Exception as e:
            logger.error(
                f"  Failed to fetch bus stops near {station_id}: "
                f"{type(e).__name__}: {e}"
            )
            return []

# ---------------------------------------------------------------------------
# Neo4j Write Operations
# ---------------------------------------------------------------------------

def create_constraints(conn: Neo4jConnection):
    """
    Creates uniqueness constraints in Neo4j.
    Ensures MERGE operations are fast and prevents duplicates.
    Safe to re-run — Neo4j skips if constraints already exist.
    """
    constraints = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (s:TubeStation) REQUIRE s.station_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (b:BusStop)     REQUIRE b.stop_id    IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (r:BusRoute)    REQUIRE r.route_id   IS UNIQUE",
    ]
    for constraint in constraints:
        conn.query(constraint)
    logger.info("Neo4j constraints created.")


def upsert_station(conn: Neo4jConnection, station: dict):
    """
    Creates or updates a TubeStation node in Neo4j.
    MERGE matches on station_id — safe to re-run.
    """
    cypher = """
        MERGE (s:TubeStation {station_id: $station_id})
        SET s.name = $name,
            s.lat  = $lat,
            s.lon  = $lon
    """
    conn.query(cypher, {
        "station_id": station["id"],
        "name":       station["commonName"],
        "lat":        station["lat"],
        "lon":        station["lon"],
    })


def upsert_bus_stop_and_relationship(
    conn:       Neo4jConnection,
    station_id: str,
    stop:       dict,
    distance_m: float
):
    """
    Creates or updates a BusStop node and the HAS_NEARBY_STOP relationship
    between it and its parent TubeStation.
    Also creates BusRoute nodes and SERVED_BY relationships from stop lines.
    """
    # Upsert the BusStop node and relationship to TubeStation
    cypher = """
        MATCH  (s:TubeStation {station_id: $station_id})
        MERGE  (b:BusStop {stop_id: $stop_id})
        SET    b.name = $name,
               b.lat  = $lat,
               b.lon  = $lon
        MERGE  (s)-[r:HAS_NEARBY_STOP]->(b)
        SET    r.distance_metres = $distance_m
    """
    conn.query(cypher, {
        "station_id": station_id,
        "stop_id":    stop["id"],
        "name":       stop["commonName"],
        "lat":        stop["lat"],
        "lon":        stop["lon"],
        "distance_m": distance_m,
    })

    # Upsert BusRoute nodes and SERVED_BY relationships
    for line in stop.get("lines", []):
        route_cypher = """
            MATCH  (b:BusStop {stop_id: $stop_id})
            MERGE  (r:BusRoute {route_id: $route_id})
            SET    r.name = $route_name
            MERGE  (b)-[:SERVED_BY]->(r)
        """
        conn.query(route_cypher, {
            "stop_id":    stop["id"],
            "route_id":   line["id"],
            "route_name": line["name"],
        })

# ---------------------------------------------------------------------------
# Main Seeding Orchestrator
# ---------------------------------------------------------------------------

async def seed():
    """
    Main orchestrator. Fetches all TfL data and writes it to Neo4j.
    """
    logger.info("Starting Neo4j graph seeding...")

    async with aiohttp.ClientSession() as session:

        # Step 1: Fetch all Tube stations
        stations = await fetch_tube_stations(session)
        if not stations:
            logger.error("No stations fetched. Aborting.")
            return

        # Step 2: Fetch nearby bus stops for all stations concurrently
        semaphore        = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        tasks            = [
            fetch_nearby_bus_stops(
                session, s["id"], s["lat"], s["lon"], semaphore
            )
            for s in stations
        ]
        all_nearby_stops = await asyncio.gather(*tasks)

        # Step 3: Write everything to Neo4j
        with Neo4jConnection() as conn:
            create_constraints(conn)

            for station, nearby_stops in zip(stations, all_nearby_stops):
                # Write the station node
                upsert_station(conn, station)

                # Write each nearby bus stop + relationship
                for stop in nearby_stops:
                    distance_m = stop.get("distance", 0)
                    upsert_bus_stop_and_relationship(
                        conn, station["id"], stop, distance_m
                    )

            # Summary query
            summary = conn.query("""
                MATCH (s:TubeStation) WITH count(s) AS stations
                MATCH (b:BusStop)    WITH stations, count(b) AS bus_stops
                MATCH (r:BusRoute)   RETURN stations, bus_stops, count(r) AS routes
            """)
            logger.info(f"Graph seeding complete. Summary: {summary}")

if __name__ == "__main__":
    asyncio.run(seed())