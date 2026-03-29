import logging
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()

logger = logging.getLogger(__name__)

class Neo4jConnection:
    def __init__(self):
        self._uri      = os.getenv("NEO4J_URI")
        self._username = os.getenv("NEO4J_USERNAME")
        self._password = os.getenv("NEO4J_PASSWORD")
        self._driver   = None

        if not all([self._uri, self._username, self._password]):
            raise ValueError(
                "Missing Neo4j credentials. Check NEO4J_URI, "
                "NEO4J_USERNAME, NEO4J_PASSWORD in your .env file."
            )

    def __enter__(self):
        self._driver = GraphDatabase.driver(
            self._uri,
            auth=(self._username, self._password)
        )
        # Verify connectivity immediately — fail fast if credentials are wrong
        self._driver.verify_connectivity()
        logger.info("Neo4j connection established.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._driver:
            self._driver.close()
            logger.info("Neo4j connection closed.")

    def query(self, cypher: str, parameters: dict = None) -> list:
        with self._driver.session() as session:
            result = session.run(cypher, parameters or {})
            return [record.data() for record in result]