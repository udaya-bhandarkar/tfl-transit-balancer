import asyncio
import aiohttp
import json
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import os

from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

load_dotenv()

TFL_BASE_URL    = "https://api.tfl.gov.uk"
TFL_APP_KEY     = os.getenv("TFL_APP_KEY")

EVENT_HUB_CONNECTION_STRING = os.getenv("EVENT_HUB_CONNECTION_STRING")
EVENT_HUB_NAME              = os.getenv("EVENT_HUB_NAME")

DISRUPTION_SEVERITY_THRESHOLD = 10
POLL_INTERVAL_SECONDS         = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)
logger = logging.getLogger(__name__)

logging.getLogger("azure.eventhub").setLevel(logging.WARNING)
logging.getLogger("uamqp").setLevel(logging.WARNING)

async def fetch_tube_line_status(session: aiohttp.ClientSession) -> list[dict]:
    url    = f"{TFL_BASE_URL}/Line/Mode/tube/Status"
    params = {"app_key": TFL_APP_KEY}

    try:
        async with session.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as response:
            response.raise_for_status()
            data = await response.json()
            logger.info(f"Fetched status for {len(data)} Tube lines.")
            return data

    except aiohttp.ClientResponseError as e:
        logger.error(f"TfL API HTTP error: {e.status} - {e.message}")
        return []

    except asyncio.TimeoutError:
        logger.error("TfL API request timed out after 10s.")
        return []

    except Exception as e:
        logger.error(f"Unexpected error fetching line status: {e}")
        return []

#Data Transformation - Raw API data to structured events
def parse_line_status(raw_lines: list[dict]) -> list[dict]:
    events    = []
    timestamp = datetime.now(timezone.utc).isoformat()

    for line in raw_lines:
        line_id   = line.get("id", "unknown")
        line_name = line.get("name", "Unknown Line")
        statuses  = line.get("lineStatuses", [])

        if not statuses:
            logger.warning(f"No status data returned for line: {line_name}")
            continue

        worst_status    = min(statuses, key=lambda s: s.get("statusSeverity", 99))
        severity_code   = worst_status.get("statusSeverity", 10)
        severity_desc   = worst_status.get("statusSeverityDescription", "Unknown")
        disruption_text = worst_status.get("disruption", {}).get("description", None)

        event = {
            "event_type":        "tube_line_status",
            "line_id":           line_id,
            "line_name":         line_name,
            "severity_code":     severity_code,
            "severity_desc":     severity_desc,
            "is_disrupted":      severity_code < DISRUPTION_SEVERITY_THRESHOLD,
            "disruption_detail": disruption_text,
            "ingested_at_utc":   timestamp,
        }

        events.append(event)

    return events

#Publish to Event Hubs
async def publish_to_event_hub(events: list[dict]) -> bool:
    """
    Publishes a list of event dicts to Azure Event Hubs as a single batch.

    Batching is important — it sends all 11 line status events in one
    network round trip rather than 11 separate calls. Much more efficient.

    Args:
        events: List of parsed event dicts to publish.

    Returns:
        True if publish succeeded, False otherwise.
    """
    try:
        # Producer is created fresh each poll cycle — this is intentional.
        # It avoids stale connection issues on long-running processes.
        async with EventHubProducerClient.from_connection_string(
            conn_str=EVENT_HUB_CONNECTION_STRING,
            eventhub_name=EVENT_HUB_NAME
        ) as producer:

            # Create a batch — Event Hubs will raise if batch exceeds 1MB
            batch = await producer.create_batch()

            for event in events:
                # Serialise dict to JSON string, encode to bytes
                event_body = json.dumps(event).encode("utf-8")
                batch.add(EventData(event_body))

            await producer.send_batch(batch)
            logger.info(
                f"Published batch of {len(events)} events to "
                f"Event Hub '{EVENT_HUB_NAME}'."
            )
            return True

    except Exception as e:
        logger.error(f"Failed to publish to Event Hubs: {type(e).__name__}: {e}")
        return False

#Main Polling Loop
async def run_polling_loop():
    """
    Main async loop: fetches TfL line status and publishes to Event Hubs
    every POLL_INTERVAL_SECONDS.
    """
    # Validate credentials on startup — fail fast before entering the loop
    if not EVENT_HUB_CONNECTION_STRING or not EVENT_HUB_NAME:
        raise ValueError(
            "Missing Event Hubs credentials. Check EVENT_HUB_CONNECTION_STRING "
            "and EVENT_HUB_NAME in your .env file."
        )

    logger.info("Starting TfL Line Status polling loop...")
    logger.info(
        f"Poll interval: {POLL_INTERVAL_SECONDS}s | "
        f"Target Event Hub: {EVENT_HUB_NAME}"
    )

    async with aiohttp.ClientSession() as session:
        while True:
            raw_data = await fetch_tube_line_status(session)

            if raw_data:
                events    = parse_line_status(raw_data)
                disrupted = [e for e in events if e["is_disrupted"]]

                # Log a readable summary to console for monitoring
                logger.info(
                    f"Lines OK: {len(events) - len(disrupted)} | "
                    f"Disrupted: {len(disrupted)}"
                )
                for e in disrupted:
                    logger.warning(
                        f"  🔴 {e['line_name']} — {e['severity_desc']}"
                    )

                # Publish to Event Hubs
                success = await publish_to_event_hub(events)

                if not success:
                    # Log the failure but keep the loop running —
                    # a transient network error shouldn't kill the pipeline
                    logger.warning(
                        "Publish failed this cycle. Will retry next poll."
                    )

            logger.info(f"Sleeping {POLL_INTERVAL_SECONDS}s until next poll...\n")
            await asyncio.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(run_polling_loop())