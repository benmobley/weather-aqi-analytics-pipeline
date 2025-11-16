# Weather + Air Quality Analytics Pipeline

A comprehensive data engineering project that collects real-time weather and air quality data from APIs, stores it in PostgreSQL, transforms it using dbt, and visualizes it through an interactive Streamlit dashboard.

## 🌟 Features

- **Real-time Data Collection**: Automated fetching of weather data from OpenWeatherMap API for 10 major US cities
- **Air Quality Integration**: Optional AirNow API integration for comprehensive environmental data
- **Robust Data Warehouse**: PostgreSQL database with raw, staging, and mart schemas
- **Data Transformation**: dbt models with data quality tests and documentation
- **Interactive Dashboard**: Streamlit-powered analytics with real-time visualizations
- **Fully Containerized**: Complete Docker setup for seamless deployment and scaling

## 🏗️ Architecture

```
OpenWeatherMap API → Python ETL → PostgreSQL → dbt → Streamlit Dashboard
     (Weather)         ↓          (Raw Data)     ↓      (Analytics)
AirNow API --------→ JSON Storage → Staging → Marts → Visualizations
  (Air Quality)                      ↓         ↓
                                   Views    Tables
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- OpenWeatherMap API key (free at openweathermap.org)
- AirNow API key (optional, free at airnowapi.org)

### 1. Environment Setup

```bash
# Navigate to project directory
cd weather-aqi-analytics-pipeline

# Configure your API keys in .env file
OPENWEATHER_API_KEY=your_openweather_key_here
AIRNOW_API_KEY=your_airnow_key_here  # Optional
DB_HOST=postgres
DB_PORT=5432
DB_USER=weather_user
DB_PASSWORD=weather_pass
DB_NAME=weather_db
```

### 2. Launch Infrastructure

```bash
# Start all services (PostgreSQL, ETL App, dbt, Streamlit)
docker-compose up -d

# Verify all services are running
docker-compose ps
# Expected: postgres (healthy), app, dbt, streamlit (all "Up")
```

### 3. Initialize Data Pipeline

```bash
# Install dbt dependencies (one-time setup)
docker-compose exec dbt dbt deps

# Run ETL pipeline to collect weather data
docker-compose exec app python -m src.etl.fetch_and_load_weather
docker-compose exec app python -m src.etl.fetch_and_load_weather

# Or run locally (after installing dependencies)
pip install -e .
python -m src.etl.fetch_and_load_weather
```

### 4. Transform Data with dbt

```bash
# Run dbt models
docker-compose exec app bash -c "cd dbt_weather && dbt run"

# Or run locally
cd dbt_weather
dbt run
```

### 5. Access Dashboard

Navigate to [http://localhost:8501](http://localhost:8501) to view the interactive Streamlit dashboard with weather analytics and visualizations.

## Project Structure

```
├── README.md
├── docker-compose.yml
├── pyproject.toml
├── .env.example
├── src/
│   ├── config.py          # Configuration and environment variables
│   ├── db.py              # Database connection utilities
│   ├── clients/           # API clients
│   │   ├── weather_client.py
│   │   └── air_quality_client.py
│   ├── etl/              # ETL processes
│   │   ├── fetch_and_load_weather.py
│   │   └── transform_to_analytics.py
│   └── scheduling/       # Orchestration examples
│       └── airflow_dag_example.py
└── dbt_weather/          # dbt project
    ├── dbt_project.yml
    ├── profiles.yml
    └── models/
        ├── staging/
        └── marts/
```

## Environment Variables

Create a `.env` file with:

```
# Database
DB_HOST=localhost
DB_PORT=5432
DB_USER=weather_user
DB_PASSWORD=weather_pass
DB_NAME=weather_db

# API Keys
OPENWEATHER_API_KEY=your_openweather_api_key
AIRNOW_API_KEY=your_airnow_api_key
```

## Development

### Local Development Setup

```bash
# Install dependencies
pip install -e .

# Run tests (when added)
pytest

# Run linting
black src/
flake8 src/
```

### Adding New Cities

Edit `src/config.py` to add cities to the `CITIES` list.

### Extending the Pipeline

1. Add new API clients in `src/clients/`
2. Create new ETL processes in `src/etl/`
3. Add dbt models in `dbt_weather/models/`
4. Schedule with Airflow/Prefect in `src/scheduling/`

## Services

- **PostgreSQL**: Data warehouse (port 5432)
- **Metabase**: BI tool (port 3000)
- **App**: Python ETL container

## License

MIT License
