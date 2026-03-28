import asyncio
import aiohttp
import json
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import os

load_dotenv()

TFL_BASE_URL = "https://api.tfl.gov.uk"
TFL_APP_KEY  = os.getenv("TFL_APP_KEY")

# Severity codes below this threshold are considered disruptions.
# TfL uses 0-20; 10 = Good Service. Lower = worse.
# Ref: https://api.tfl.gov.uk/Line/Meta/Severity
DISRUPTION_SEVERITY_THRESHOLD = 10

# How often to poll (seconds). 30s is respectful to the API
# and well within our 500 req/min budget.
POLL_INTERVAL_SECONDS = 30

# Logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)
logger = logging.getLogger(__name__)

async def fetch_tube_line_status(session: aiohttp.ClientSession) -> list[dict]:
    """
    Makes a single async GET request to the TfL Line Status endpoint.

    Args:
        session: A shared aiohttp ClientSession (reuse for connection pooling).

    Returns:
        Raw list of line status objects from the TfL API, or empty list on error.
    """
    url    = f"{TFL_BASE_URL}/Line/Mode/tube/Status"
    params = {"app_key": TFL_APP_KEY}

    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
            response.raise_for_status()  # Raises on 4xx / 5xx
            data = await response.json()
            logger.info(f"Fetched status for {len(data)} Tube lines.")
            return data

    except aiohttp.ClientResponseError as e:
        # HTTP-level errors (401 bad key, 429 rate limited, 503 TfL down)
        logger.error(f"TfL API HTTP error: {e.status} - {e.message}")
        return []

    except asyncio.TimeoutError:
        logger.error("TfL API request timed out after 10s.")
        return []

    except Exception as e:
        logger.error(f"Unexpected error fetching line status: {e}")
        return []

# Data Transformation

def parse_line_status(raw_lines: list[dict]) -> list[dict]:
    """
    Transforms raw TfL API response into clean, flat event dictionaries.

    Each dict represents one line's status snapshot — shaped for easy
    serialisation to JSON and onward publishing to Event Hubs.

    Args:
        raw_lines: Raw list of line objects from the TfL API.

    Returns:
        List of structured event dicts, one per Tube line.
    """
    events    = []
    timestamp = datetime.now(timezone.utc).isoformat()  # ISO-8601 UTC

    for line in raw_lines:
        line_id   = line.get("id", "unknown")
        line_name = line.get("name", "Unknown Line")

        # A single line can have multiple concurrent statuses (e.g. part-closure
        # AND minor delays). We capture the worst (lowest severity code).
        statuses = line.get("lineStatuses", [])

        if not statuses:
            logger.warning(f"No status data returned for line: {line_name}")
            continue

        # Sort by severity ascending (0 = most severe), take the worst
        worst_status     = min(statuses, key=lambda s: s.get("statusSeverity", 99))
        severity_code    = worst_status.get("statusSeverity", 10)
        severity_desc    = worst_status.get("statusSeverityDescription", "Unknown")
        disruption_text  = (
            worst_status.get("disruption", {}).get("description", None)
        )

        event = {
            # --- Identity ---
            "event_type":        "tube_line_status",
            "line_id":           line_id,
            "line_name":         line_name,

            # --- Status ---
            "severity_code":     severity_code,
            "severity_desc":     severity_desc,
            "is_disrupted":      severity_code < DISRUPTION_SEVERITY_THRESHOLD,
            "disruption_detail": disruption_text,

            # --- Temporal ---
            "ingested_at_utc":   timestamp,
        }

        events.append(event)

    return events

# Main Polling Loop

async def run_polling_loop():
    """
    Main async loop: fetches and parses TfL line status every POLL_INTERVAL_SECONDS.
    """
    logger.info("Starting TfL Line Status polling loop...")
    logger.info(f"Poll interval: {POLL_INTERVAL_SECONDS}s | "
                f"Disruption threshold: severity < {DISRUPTION_SEVERITY_THRESHOLD}")

    # Single session reused across all polls — avoids TCP handshake overhead
    async with aiohttp.ClientSession() as session:
        while True:
            raw_data = await fetch_tube_line_status(session)

            if raw_data:
                events    = parse_line_status(raw_data)
                disrupted = [e for e in events if e["is_disrupted"]]

                # --- Output (temporary: console) ---
                print(f"  TUBE STATUS SNAPSHOT — {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")

                for event in events:
                    status_icon = "🔴" if event["is_disrupted"] else "🟢"
                    print(f"  {status_icon}  {event['line_name']:<25} | "
                          f"{event['severity_desc']}")
                    if event["disruption_detail"]:
                        # Trim long TfL disruption strings for readability
                        detail = event["disruption_detail"][:120] + "..."
                        print(f"       ↳ {detail}")

                print(f"\n  Lines OK: {len(events) - len(disrupted)} | "
                      f"Disrupted: {len(disrupted)}")

                # Pretty-print first event as a sample of the data shape
                if events:
                    print("\n  [Sample event payload]")
                    print(json.dumps(events[0], indent=4))

            logger.info(f"Sleeping {POLL_INTERVAL_SECONDS}s until next poll...\n")
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    asyncio.run(run_polling_loop())