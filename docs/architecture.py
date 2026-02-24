"""
Visual representation of the pipeline architecture
"""

PIPELINE_ARCHITECTURE = """
╔═══════════════════════════════════════════════════════════════════════╗
║                    WEATHER DATA PIPELINE ARCHITECTURE                  ║
╚═══════════════════════════════════════════════════════════════════════╝

┌─────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                   │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  │ API Requests (HTTP/JSON)
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     EXTRACT LAYER (src/extract/)                        │
│  ┌──────────────────────────────────────────────────────────────┐      │
│  │  WeatherExtractor                                            │      │
│  │  • Fetch current weather data                                │      │
│  │  • Fetch forecast data                                       │      │
│  │  • Retry logic with exponential backoff                      │      │
│  │  • Rate limiting & error handling                            │      │
│  └──────────────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  │ Raw JSON Data
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                   TRANSFORM LAYER (src/transform/)                      │
│  ┌──────────────────────────────────────────────────────────────┐      │
│  │  WeatherTransformer                                          │      │
│  │  • Parse and extract fields                                  │      │
│  │  • Data validation & cleaning                                │      │
│  │  • Remove duplicates                                         │      │
│  │  • Add derived features                                      │      │
│  │    - Temperature categories                                  │      │
│  │    - Humidity levels                                         │      │
│  │    - Wind speed categories                                   │      │
│  │  • Aggregate and summarize data                              │      │
│  └──────────────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  │ Structured DataFrame
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     LOAD LAYER (src/load/)                              │
│  ┌──────────────────────────────────────────────────────────────┐      │
│  │  DataLoader                                                  │      │
│  │  • CSV export                                                │      │
│  │  • SQLite database                                           │      │
│  │  • PostgreSQL database                                       │      │
│  │  • JSON files                                                │      │
│  │  • Parquet files                                             │      │
│  │  • Automated backups                                         │      │
│  └──────────────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  │
      ┌───────────────┬───────────┴─────────┬──────────────┐
      │               │                     │              │
      ▼               ▼                     ▼              ▼
┌──────────┐    ┌──────────┐         ┌──────────┐   ┌──────────┐
│   CSV    │    │  SQLite  │         │PostgreSQL│   │   JSON   │
│  Files   │    │    DB    │         │    DB    │   │  Files   │
└──────────┘    └──────────┘         └──────────┘   └──────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                    SUPPORTING COMPONENTS                                │
│                                                                         │
│  • Logger (src/utils/logger.py)                                        │
│    - Console & file logging                                            │
│    - Daily log rotation                                                │
│    - Multiple log levels                                               │
│                                                                         │
│  • Configuration (config.yaml)                                         │
│    - Pipeline settings                                                 │
│    - Storage options                                                   │
│    - Retry policies                                                    │
│                                                                         │
│  • Environment Variables (.env)                                        │
│    - API credentials                                                   │
│    - Database connections                                              │
│    - Runtime settings                                                  │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATION                                    │
│                                                                         │
│  • pipeline.py - Main ETL orchestrator                                 │
│  • scheduler.py - Automated scheduled execution                        │
└─────────────────────────────────────────────────────────────────────────┘
"""

DATA_FLOW = """
╔═══════════════════════════════════════════════════════════════════════╗
║                          DATA FLOW DIAGRAM                             ║
╚═══════════════════════════════════════════════════════════════════════╝

API Response (JSON)                 Transformed Data                 Output
─────────────────                   ────────────────                 ──────

{                                   city             | London
  "name": "London",                 country          | GB
  "main": {                         latitude         | 51.51
    "temp": 15.5,         ────►     longitude        | -0.13         ────►  CSV
    "humidity": 65                  temperature      | 15.5                 SQLite
  },                                feels_like       | 14.2                 PostgreSQL
  "weather": [{                     humidity         | 65                   JSON
    "main": "Clear"                 temp_category    | Moderate             Parquet
  }],                               humidity_category| Moderate
  "wind": {                         wind_speed       | 5.2
    "speed": 5.2                    wind_category    | Light
  }                                 weather_main     | Clear
}                                   timestamp        | 2026-02-24
"""

COMPONENT_INTERACTION = """
╔═══════════════════════════════════════════════════════════════════════╗
║                      COMPONENT INTERACTION                             ║
╚═══════════════════════════════════════════════════════════════════════╝

┌──────────────┐
│   pipeline.py│
│  (Main Entry)│
└──────┬───────┘
       │
       ├─────────► Load .env & config.yaml
       │
       ├─────────► Initialize Logger
       │                  │
       │                  ├─► Create log files
       │                  └─► Setup handlers
       │
       ├─────────► Initialize Extractor
       │                  │
       │                  ├─► Setup API client
       │                  └─► Configure retries
       │
       ├─────────► Initialize Transformer
       │                  │
       │                  └─► Prepare data processors
       │
       ├─────────► Initialize Loader
       │                  │
       │                  └─► Setup storage connectors
       │
       ├─────────► Execute Pipeline
       │                  │
       │                  ├─► Extract data (API calls)
       │                  │
       │                  ├─► Transform data (cleaning & enrichment)
       │                  │
       │                  └─► Load data (save to storage)
       │
       └─────────► Log results & cleanup
"""

def print_architecture():
    """Print the pipeline architecture"""
    print(PIPELINE_ARCHITECTURE)
    print("\n" + DATA_FLOW)
    print("\n" + COMPONENT_INTERACTION)


if __name__ == '__main__':
    print_architecture()
