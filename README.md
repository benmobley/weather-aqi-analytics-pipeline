# Weather + Air Quality Analytics Pipeline

A real-time data engineering pipeline that collects weather and air quality data from APIs, transforms it with dbt, and visualizes it through an interactive Streamlit dashboard.

## 🚀 Quick Start

### 1. Setup Environment
```bash
# Clone and navigate to project
cd weather-aqi-analytics-pipeline

# Configure API keys in .env
OPENWEATHER_API_KEY=your_openweather_key_here
AIRNOW_API_KEY=your_airnow_key_here  # Optional
```

### 2. Launch Services
```bash
# Start all services (PostgreSQL, ETL, dbt, Streamlit)
docker-compose up -d

# Verify services are running
docker-compose ps
```

### 3. Initialize Pipeline
```bash
# Install dbt dependencies (one-time)
docker-compose exec dbt dbt deps

# Run ETL to collect weather data
docker-compose exec app python -m src.etl.fetch_and_load_weather

# Transform data with dbt
docker-compose exec dbt dbt run
```

### 4. Access Dashboard
Visit **http://localhost:8501** for interactive weather analytics.

## 🏗️ Architecture

```
OpenWeatherMap API → Python ETL → PostgreSQL → dbt → Streamlit Dashboard
     (Weather)         ↓          (Raw Data)     ↓      (Analytics)
AirNow API --------→ JSON Storage → Staging → Marts → Visualizations
```

**Data Flow**: APIs → Raw Storage → Staging Views → Mart Tables → Interactive Dashboard

## 📊 Features

- **Real-time Data**: Weather data for 10 major US cities, updated on-demand
- **Interactive Dashboard**: Temperature trends, city comparisons, weather distributions
- **Data Quality**: Automated testing and validation with dbt
- **Containerized**: Complete Docker setup for easy deployment

## 🛠️ Key Commands

### ETL Operations
```bash
# Standard run
docker-compose exec app python -m src.etl.fetch_and_load_weather

# Verbose logging
docker-compose exec app python -m src.etl.fetch_and_load_weather --verbose

# Specific cities
docker-compose exec app python -m src.etl.fetch_and_load_weather --cities "Paris,FR"
```

### dbt Operations
```bash
# Run transformations
docker-compose exec dbt dbt run

# Run data tests
docker-compose exec dbt dbt test

# Specific models
docker-compose exec dbt dbt run --models staging
```

### Monitoring
```bash
# Service status
docker-compose ps

# View logs
docker logs weather_streamlit --tail 20
docker logs weather_app --tail 20

# Database access
docker-compose exec postgres psql -U weather_user -d weather_db
```

## 📁 Project Structure

```
weather-aqi-analytics-pipeline/
├── src/                          # Python ETL application
│   ├── clients/                  # API integrations (OpenWeather, AirNow)
│   └── etl/                      # Data pipeline processes
├── dbt_weather/                  # dbt transformation project
│   └── models/
│       ├── staging/              # Data cleaning & standardization
│       └── marts/                # Business logic & aggregations
├── dashboard/                    # Streamlit dashboard
│   └── app.py                    # Interactive analytics interface
├── docker-compose.yml            # Service orchestration
└── .env                          # API keys & configuration
```

## 🔧 Configuration

### API Keys (Required)
- **OpenWeatherMap**: Free at [openweathermap.org](https://openweathermap.org/api) (1,000 calls/day)
- **AirNow**: Optional at [airnowapi.org](https://www.airnowapi.org/aq/account/request/) (500 requests/hour)

### Database Schema
- **Raw**: JSON API responses stored as-is
- **Staging**: Cleaned and standardized data views
- **Marts**: Business logic tables for analytics

### Adding Cities
Edit `DEFAULT_CITIES` in `src/config.py`:
```python
DEFAULT_CITIES = [
    "New York,US", "London,GB", "Tokyo,JP",
    # Add cities in "City,CountryCode" format
]
```

## 🚨 Troubleshooting

| Issue | Solution |
|-------|----------|
| API Rate Limits | Check free tier limits: OpenWeather (1K/day), AirNow (500/hour) |
| Database Connection | `docker-compose restart postgres app` |
| dbt Failures | `docker-compose exec dbt dbt run --debug` |
| Dashboard Not Loading | `docker-compose restart streamlit` |

## 🏗️ Production Notes

- **Scheduling**: Use cron or Airflow for regular ETL runs
- **Security**: Implement secrets management for API keys
- **Scaling**: Consider managed databases (AWS RDS, Google Cloud SQL)
- **Monitoring**: Add logging and alerting systems

## 📈 Dashboard Highlights

- **Weather Metrics**: Temperature, humidity, pressure, wind speed
- **Interactive Charts**: Time series, comparisons, distributions
- **Smart Filters**: City selection, date ranges
- **Live Updates**: Auto-refresh every 5 minutes

---

**Built with**: Python • PostgreSQL • dbt • Streamlit • Docker  
**Data Sources**: OpenWeatherMap API • AirNow API  
**Ready to Deploy**: Complete containerized solution