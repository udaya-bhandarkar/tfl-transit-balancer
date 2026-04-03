"""
seed_graph.py

Responsibility: One-time script to populate Neo4j with the TfL network graph.

What it builds:
    (:TubeLine)    -[:SERVES]->                          (:TubeStation)
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

# Configuration
TFL_BASE_URL            = "https://api.tfl.gov.uk"
TFL_APP_KEY             = os.getenv("TFL_APP_KEY")
NEARBY_STOP_RADIUS_M    = 300
MAX_CONCURRENT_REQUESTS = 5

TUBE_LINES = [
    "bakerloo", "central", "circle", "district",
    "hammersmith-city", "jubilee", "metropolitan",
    "northern", "piccadilly", "victoria", "waterloo-city"
]

# TfL API Calls

async def fetch_tube_stations(session: aiohttp.ClientSession) -> list[dict]:
    """
    Fetches all Tube stations filtered to NaptanMetroStation type.
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
    url    = f"{TFL_BASE_URL}/StopPoint"
    params = {
        "lat":       lat,
        "lon":       lon,
        "stopTypes": "NaptanPublicBusCoachTram",
        "radius":    NEARBY_STOP_RADIUS_M,
        "app_key":   TFL_APP_KEY
    }

    async with semaphore:
        await asyncio.sleep(0.5)
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


# TfL API Call — Line to Station Relationships

async def fetch_line_stations(
    session:   aiohttp.ClientSession,
    line_id:   str,
    semaphore: asyncio.Semaphore
) -> dict:
    url    = f"{TFL_BASE_URL}/Line/{line_id}/StopPoints"
    params = {"app_key": TFL_APP_KEY}

    async with semaphore:
        await asyncio.sleep(0.5)
        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

                station_ids = [
                    stop["id"] for stop in data
                    if stop.get("stopType") == "NaptanMetroStation"
                ]

                logger.info(
                    f"  Line {line_id}: serves {len(station_ids)} stations."
                )
                return {
                    "line_id":    line_id,
                    "line_name":  line_id.replace("-", " ").title(),
                    "station_ids": station_ids
                }

        except Exception as e:
            logger.error(
                f"  Failed to fetch stations for line {line_id}: "
                f"{type(e).__name__}: {e}"
            )
            return {"line_id": line_id, "line_name": line_id, "station_ids": []}


# Neo4j Write Operations

def create_constraints(conn: Neo4jConnection):
    constraints = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (s:TubeStation) REQUIRE s.station_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (b:BusStop)     REQUIRE b.stop_id    IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (r:BusRoute)    REQUIRE r.route_id   IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (l:TubeLine)    REQUIRE l.line_id    IS UNIQUE",
    ]
    for constraint in constraints:
        conn.query(constraint)
    logger.info("Neo4j constraints created.")


def upsert_station(conn: Neo4jConnection, station: dict):
    """Creates or updates a TubeStation node."""
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
    """Creates or updates BusStop nodes and relationships."""
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


# Neo4j Write — TubeLine nodes and SERVES relationships

def upsert_tube_line_and_relationships(
    conn:      Neo4jConnection,
    line_data: dict
):
    line_id     = line_data["line_id"]
    line_name   = line_data["line_name"]
    station_ids = line_data["station_ids"]

    if not station_ids:
        logger.warning(f"No stations to link for line: {line_id}")
        return

    # Create the TubeLine node
    line_cypher = """
        MERGE (l:TubeLine {line_id: $line_id})
        SET l.name = $line_name
    """
    conn.query(line_cypher, {
        "line_id":   line_id,
        "line_name": line_name
    })

    # Create SERVES relationships to each station
    serves_cypher = """
        MATCH (l:TubeLine  {line_id:    $line_id})
        MATCH (s:TubeStation {station_id: $station_id})
        MERGE (l)-[:SERVES]->(s)
    """
    linked   = 0
    skipped  = 0

    for station_id in station_ids:
        try:
            conn.query(serves_cypher, {
                "line_id":    line_id,
                "station_id": station_id
            })
            linked += 1
        except Exception as e:
            skipped += 1
            logger.warning(f"Could not link {station_id} to {line_id}: {e}")

    logger.info(
        f"  Line {line_id}: linked {linked} stations, skipped {skipped}."
    )


async def seed():
    logger.info("Starting Neo4j graph seeding...")

    async with aiohttp.ClientSession() as session:

        # Step 1: Fetch all Tube stations
        stations = await fetch_tube_stations(session)
        if not stations:
            logger.error("No stations fetched. Aborting.")
            return

        # Step 2: Fetch nearby bus stops for all stations concurrently
        semaphore        = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        bus_tasks        = [
            fetch_nearby_bus_stops(
                session, s["id"], s["lat"], s["lon"], semaphore
            )
            for s in stations
        ]
        all_nearby_stops = await asyncio.gather(*bus_tasks)

        # Step 3 NEW: Fetch line→station relationships for all 11 lines
        logger.info("Fetching line-to-station relationships...")
        line_tasks = [
            fetch_line_stations(session, line_id, semaphore)
            for line_id in TUBE_LINES
        ]
        all_line_data = await asyncio.gather(*line_tasks)

        # Step 4: Write everything to Neo4j
        with Neo4jConnection() as conn:
            create_constraints(conn)

            # Write station nodes and bus stop relationships
            logger.info("Writing stations and bus stops to Neo4j...")
            for station, nearby_stops in zip(stations, all_nearby_stops):
                upsert_station(conn, station)
                for stop in nearby_stops:
                    distance_m = stop.get("distance", 0)
                    upsert_bus_stop_and_relationship(
                        conn, station["id"], stop, distance_m
                    )

            # Write TubeLine nodes and SERVES relationships
            logger.info("Writing TubeLine nodes and SERVES relationships...")
            for line_data in all_line_data:
                upsert_tube_line_and_relationships(conn, line_data)

            # Summary query
            summary = conn.query("""
                MATCH (l:TubeLine)   WITH count(l) AS lines
                MATCH (s:TubeStation) WITH lines, count(s) AS stations
                MATCH (b:BusStop)    WITH lines, stations, count(b) AS bus_stops
                MATCH (r:BusRoute)   RETURN lines, stations, bus_stops, count(r) AS routes
            """)
            logger.info(f"Graph seeding complete. Summary: {summary}")


# Entry Point

if __name__ == "__main__":
    asyncio.run(seed())