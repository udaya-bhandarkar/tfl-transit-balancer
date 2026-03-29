# Real-Time Transit Load Balancer

A cloud-native, event-driven pipeline that ingests live Transport for London (TfL) 
data to detect Tube disruptions and calculate whether nearby bus routes have 
sufficient capacity to absorb displaced commuters, surfacing "Gridlock Alerts" 
on a live map.

## Stack
- **Ingestion:** Python (asyncio) → Azure Functions
- **Message Broker:** Azure Event Hubs (Kafka API)
- **Stream Processing:** Azure Databricks (PySpark Structured Streaming)
- **Graph Database:** Neo4j AuraDB
- **Frontend:** Streamlit + Folium

## Status
🚧 In active development