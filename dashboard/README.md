# Weather & Air Quality Analytics Dashboard

A comprehensive Streamlit dashboard for visualizing weather and air quality data collected from OpenWeatherMap APIs.

## Features

### 📊 Real-time Visualizations
- **Temperature Trends**: Interactive line charts showing temperature changes over time for multiple cities
- **Current Weather Comparison**: Bar charts comparing current temperature and humidity across cities
- **Weather Conditions Distribution**: Pie chart showing the distribution of different weather conditions
- **Wind Analysis**: Histogram of wind speed distribution across all observations

### 🎛️ Interactive Controls
- **City Filter**: Multi-select dropdown to choose which cities to display
- **Date Range Filter**: Date picker to filter data by time period
- **Auto-refresh**: Data refreshes every 5 minutes automatically

### 📈 Key Metrics
- Average temperature across selected cities and time period
- Average humidity percentage
- Average atmospheric pressure
- Average wind speed

### 📋 Data Tables
- **Recent Observations**: Latest weather data for each monitored city
- **Raw Data Explorer**: Database statistics and sample raw data records

## Access

The dashboard is available at: **http://localhost:8501**

## Data Sources

- **Weather Data**: OpenWeatherMap Current Weather API (Free Tier)
- **Air Quality Data**: OpenWeatherMap Air Pollution API
- **Storage**: PostgreSQL database with real-time ETL pipeline

## Technical Stack

- **Frontend**: Streamlit 1.28+
- **Visualization**: Plotly Express & Graph Objects
- **Data Processing**: Pandas 2.0+
- **Database**: PostgreSQL with SQLAlchemy
- **Containerization**: Docker & Docker Compose

## Usage

1. **Start the services**: All services are managed via Docker Compose
2. **Access the dashboard**: Navigate to http://localhost:8501
3. **Explore data**: Use the sidebar filters to customize your view
4. **Real-time updates**: Data automatically refreshes based on the ETL pipeline schedule

## Data Refresh

- **ETL Pipeline**: Runs every hour to collect new weather data
- **Dashboard Cache**: Refreshes every 5 minutes for optimal performance
- **Real-time Status**: Last updated timestamp shown at the bottom

## Troubleshooting

### No Data Available
If you see "No data available", run the ETL pipeline manually:
```bash
docker-compose exec app python -m src.etl.fetch_and_load_weather
```

### Connection Issues
Ensure all services are running:
```bash
docker-compose ps
```

All services (postgres, app, streamlit) should show "Up" status.

### Performance
- The dashboard caches data for 5 minutes to improve performance
- Large date ranges may take longer to load
- Use city filters to focus on specific locations for faster rendering