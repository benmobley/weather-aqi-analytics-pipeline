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
# This loads weather data for 10 major US cities

# Transform data with dbt
docker-compose exec dbt dbt run
# Creates staging views and mart tables from raw data
```

### 4. Access Interactive Dashboard

```bash
# Dashboard is automatically available at:
open http://localhost:8501

# Or visit: http://localhost:8501 in your browser
```

## 📊 Dashboard Features

Your Streamlit dashboard provides:

- **Real-time Weather Metrics**: Temperature, humidity, pressure, wind speed
- **Interactive Visualizations**:
  - Temperature trends over time by city
  - Current weather comparisons across cities
  - Weather conditions distribution
  - Wind speed analysis
- **Smart Filters**: City selection, date range filtering
- **Live Data**: Auto-refreshes every 5 minutes
- **Data Quality**: Shows API success rates and data freshness

## 🔧 Advanced Usage

### ETL Pipeline Options

```bash
# Run with verbose logging
docker-compose exec app python -m src.etl.fetch_and_load_weather --verbose

# Test specific cities
docker-compose exec app python -m src.etl.fetch_and_load_weather --cities "Paris,FR" --verbose

# View ETL help
docker-compose exec app python -m src.etl.fetch_and_load_weather --help
```

### dbt Operations

```bash
# Run data quality tests
docker-compose exec dbt dbt test

# Generate documentation
docker-compose exec dbt dbt docs generate

# Run specific models
docker-compose exec dbt dbt run --models staging
```

### Monitoring & Debugging

```bash
# Check service status
docker-compose ps

# View logs
docker logs weather_app --tail 20
docker logs weather_streamlit --tail 20
docker logs weather_postgres --tail 20

# Connect to database directly
docker-compose exec postgres psql -U weather_user -d weather_db
```

## 📁 Project Structure

```
weather-aqi-analytics-pipeline/
├── README.md
├── docker-compose.yml           # Multi-service orchestration
├── Dockerfile.streamlit         # Streamlit dashboard container
├── .env                        # Environment variables & API keys
├── src/                        # Python ETL application
│   ├── config.py              # Configuration management
│   ├── db.py                  # Database utilities & connections
│   ├── clients/               # API client modules
│   │   ├── weather_client.py  # OpenWeatherMap integration
│   │   └── air_quality_client.py # AirNow API integration
│   └── etl/                   # Data pipeline processes
│       └── fetch_and_load_weather.py # Main ETL orchestrator
├── dbt_weather/               # dbt transformation project
│   ├── dbt_project.yml       # dbt configuration
│   ├── profiles.yml          # Database connection profiles
│   ├── packages.yml          # dbt package dependencies
│   └── models/
│       ├── staging/          # Data cleaning & standardization
│       │   ├── stg_weather__observations.sql
│       │   └── stg_air_quality__observations.sql
│       └── marts/            # Business logic & aggregations
│           ├── dim_city.sql  # City dimension table
│           ├── fact_weather_daily.sql # Daily weather facts
│           └── fact_air_quality_daily.sql # Daily AQI facts
└── dashboard/                 # Streamlit dashboard
    ├── app.py                # Main dashboard application
    ├── requirements.txt      # Python dependencies
    └── README.md             # Dashboard documentation
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

## 🌍 Data Sources & API Integration

### OpenWeatherMap API (Primary)

- **Free Tier**: 1,000 calls/day, 60 calls/minute
- **Data**: Current weather, forecasts, air pollution
- **Coverage**: Global coverage for any city
- **Setup**: Sign up at [openweathermap.org](https://openweathermap.org/api)

### AirNow API (Optional)

- **Free Tier**: 500 requests/hour
- **Data**: US-based air quality index (AQI) data
- **Coverage**: United States only
- **Setup**: Register at [airnowapi.org](https://www.airnowapi.org/aq/account/request/)

## 🗄️ Database Architecture

The pipeline creates a three-layer data warehouse:

### Raw Layer (`raw` schema)

- **weather_observations**: Raw JSON API responses
- **Columns**: id, city, country, lat/lng, observation_time, weather_data, air_quality_data

### Staging Layer (`staging` schema)

- **stg_weather\_\_observations**: Parsed and cleaned weather data
- **stg_air_quality\_\_observations**: Normalized air quality measurements

### Mart Layer (`marts` schema)

- **dim_city**: City dimension with geographic info
- **fact_weather_daily**: Daily weather aggregations by city
- **fact_air_quality_daily**: Daily AQI summaries by city

## 🔧 Configuration & Customization

### Environment Variables

```bash
# Required
OPENWEATHER_API_KEY=your_key_from_openweathermap_org
DB_HOST=postgres
DB_USER=weather_user
DB_PASSWORD=weather_pass
DB_NAME=weather_db

# Optional
AIRNOW_API_KEY=your_airnow_key_here
DB_PORT=5432
```

### Adding Cities

Modify the city list in `src/config.py`:

```python
DEFAULT_CITIES = [
    "New York,US", "Los Angeles,US", "Chicago,US",
    "London,GB", "Paris,FR", "Tokyo,JP",
    # Add your cities here in "City,CountryCode" format
]
```

### Scheduling ETL

For production deployment, schedule regular data collection:

```bash
# Crontab example - run every hour
0 * * * * cd /path/to/project && docker-compose exec app python -m src.etl.fetch_and_load_weather

# Or use a more robust orchestrator like Apache Airflow
```

## 🚨 Troubleshooting

### Common Issues

**API Rate Limits**

- OpenWeatherMap: 1,000 calls/day on free tier
- AirNow: 500 requests/hour
- Solution: Implement caching or upgrade to paid tiers

**Database Connection Errors**

```bash
# Check PostgreSQL is running
docker-compose exec postgres pg_isready -U weather_user

# Restart services if needed
docker-compose restart postgres app
```

**dbt Model Failures**

```bash
# Check for SQL syntax errors
docker-compose exec dbt dbt compile

# Run with debug logging
docker-compose exec dbt dbt run --debug
```

**Dashboard Not Loading**

```bash
# Check Streamlit container
docker logs weather_streamlit --tail 20

# Restart dashboard service
docker-compose restart streamlit
```

### Performance Optimization

- **Database**: Add indexes on frequently queried columns
- **ETL**: Implement incremental loading for large datasets
- **Dashboard**: Optimize query caching intervals
- **APIs**: Batch requests where possible

## 🏗️ Production Deployment

For production environments:

1. **Security**: Use secrets management (AWS Secrets Manager, Azure Key Vault)
2. **Monitoring**: Add logging and alerting (ELK stack, Prometheus)
3. **Scaling**: Consider managed databases (AWS RDS, Google Cloud SQL)
4. **Orchestration**: Use workflow managers (Apache Airflow, Prefect)
5. **CI/CD**: Implement automated testing and deployment pipelines

## 📈 What's Next?

Potential enhancements:

- **Machine Learning**: Weather prediction models
- **Alerts**: Notification system for severe weather
- **Mobile App**: React Native dashboard
- **Historical Analysis**: Long-term trend analysis
- **Integration**: Connect with IoT weather stations

## 🤝 Contributing

We welcome contributions! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**🌤️ Built with:** Python, PostgreSQL, dbt, Streamlit, Docker  
**📊 Data Sources:** OpenWeatherMap API, AirNow API  
**🚀 Ready to deploy:** Complete containerized solution
