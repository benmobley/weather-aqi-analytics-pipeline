"""
Weather + Air Quality Analytics Dashboard

A Streamlit dashboard for visualizing weather and air quality data
collected from OpenWeatherMap and AirNow APIs.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from sqlalchemy import create_engine
import os
from datetime import datetime, timedelta
import json

# Page configuration
st.set_page_config(
    page_title="Weather & Air Quality Analytics", 
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database connection
@st.cache_resource
def init_db_connection():
    """Initialize database connection."""
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_user = os.getenv('DB_USER', 'weather_user')
    db_password = os.getenv('DB_PASSWORD', 'weather_pass')
    db_name = os.getenv('DB_NAME', 'weather_db')
    
    database_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(database_url)
    return engine

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_raw_weather_data():
    """Load raw weather observations from database."""
    query = """
    SELECT 
        id, city, country, latitude, longitude, observation_time,
        weather_data::json as weather_json,
        air_quality_data::json as aqi_json,
        created_at
    FROM raw.weather_observations
    ORDER BY observation_time DESC
    LIMIT 1000
    """
    
    try:
        engine = init_db_connection()
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def extract_weather_metrics(df):
    """Extract weather metrics from JSON data."""
    if df.empty:
        return pd.DataFrame()
    
    metrics = []
    for _, row in df.iterrows():
        try:
            weather_json = row['weather_json']
            
            if 'error' in weather_json:
                continue
                
            main = weather_json.get('main', {})
            weather = weather_json.get('weather', [{}])[0]
            wind = weather_json.get('wind', {})
            
            metric = {
                'id': row['id'],
                'city': row['city'],
                'country': row['country'],
                'latitude': row['latitude'],
                'longitude': row['longitude'],
                'observation_time': row['observation_time'],
                'temperature_celsius': main.get('temp'),
                'feels_like_celsius': main.get('feels_like'),
                'humidity_percent': main.get('humidity'),
                'pressure_hpa': main.get('pressure'),
                'weather_main': weather.get('main'),
                'weather_description': weather.get('description'),
                'wind_speed_mps': wind.get('speed'),
                'wind_direction_deg': wind.get('deg'),
                'cloudiness_percent': weather_json.get('clouds', {}).get('all'),
                'visibility_meters': weather_json.get('visibility'),
                'created_at': row['created_at']
            }
            metrics.append(metric)
            
        except Exception as e:
            continue
    
    return pd.DataFrame(metrics)

def main():
    """Main dashboard function."""
    
    # Header
    st.title("🌤️ Weather & Air Quality Analytics Dashboard")
    st.markdown("Real-time weather data from OpenWeatherMap API")
    
    # Load data
    with st.spinner("Loading weather data..."):
        raw_df = load_raw_weather_data()
        
    if raw_df.empty:
        st.error("No data available. Please run the ETL pipeline first.")
        st.code("docker-compose exec app python -m src.etl.fetch_and_load_weather")
        return
    
    # Extract metrics
    weather_df = extract_weather_metrics(raw_df)
    
    if weather_df.empty:
        st.warning("No valid weather data found in the database.")
        return
    
    # Sidebar filters
    st.sidebar.header("Filters")
    
    # City filter
    cities = sorted(weather_df['city'].unique())
    selected_cities = st.sidebar.multiselect(
        "Select Cities",
        cities,
        default=cities[:5] if len(cities) > 5 else cities
    )
    
    # Date filter
    if not weather_df['observation_time'].empty:
        min_date = weather_df['observation_time'].min().date()
        max_date = weather_df['observation_time'].max().date()
        
        # Only show date filter if we have data spanning more than one day
        if min_date != max_date:
            # Ensure start date is not before min_date
            start_date_default = max(min_date, max_date - timedelta(days=7))
            
            date_range = st.sidebar.date_input(
                "Date Range",
                value=(start_date_default, max_date),
                min_value=min_date,
                max_value=max_date
            )
            
            if len(date_range) == 2:
                start_date, end_date = date_range
                weather_df = weather_df[
                    (weather_df['observation_time'].dt.date >= start_date) &
                    (weather_df['observation_time'].dt.date <= end_date)
                ]
        else:
            st.sidebar.info(f"Showing data for: {max_date}")
    
    # Filter by selected cities
    if selected_cities:
        weather_df = weather_df[weather_df['city'].isin(selected_cities)]
    
    if weather_df.empty:
        st.warning("No data matches the selected filters.")
        return
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        avg_temp = weather_df['temperature_celsius'].mean()
        st.metric(
            "Average Temperature", 
            f"{avg_temp:.1f}°C" if pd.notna(avg_temp) else "N/A",
            f"{avg_temp * 9/5 + 32:.1f}°F" if pd.notna(avg_temp) else ""
        )
    
    with col2:
        avg_humidity = weather_df['humidity_percent'].mean()
        st.metric(
            "Average Humidity", 
            f"{avg_humidity:.1f}%" if pd.notna(avg_humidity) else "N/A"
        )
    
    with col3:
        avg_pressure = weather_df['pressure_hpa'].mean()
        st.metric(
            "Average Pressure", 
            f"{avg_pressure:.0f} hPa" if pd.notna(avg_pressure) else "N/A"
        )
    
    with col4:
        avg_wind = weather_df['wind_speed_mps'].mean()
        st.metric(
            "Average Wind Speed", 
            f"{avg_wind:.1f} m/s" if pd.notna(avg_wind) else "N/A"
        )
    
    # Charts
    st.header("📊 Weather Visualizations")
    
    # Temperature over time
    st.subheader("Temperature Trends")
    
    if not weather_df.empty and 'temperature_celsius' in weather_df.columns:
        fig_temp = px.line(
            weather_df.sort_values('observation_time'),
            x='observation_time',
            y='temperature_celsius',
            color='city',
            title="Temperature Over Time",
            labels={
                'temperature_celsius': 'Temperature (°C)',
                'observation_time': 'Time'
            }
        )
        fig_temp.update_layout(height=400)
        st.plotly_chart(fig_temp, use_container_width=True)
    
    # Current weather comparison
    st.subheader("Current Weather Comparison")
    
    # Get latest data for each city
    latest_data = weather_df.loc[weather_df.groupby('city')['observation_time'].idxmax()]
    
    if not latest_data.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Temperature comparison
            fig_temp_bar = px.bar(
                latest_data.sort_values('temperature_celsius'),
                x='city',
                y='temperature_celsius',
                title="Current Temperature by City",
                labels={'temperature_celsius': 'Temperature (°C)'},
                color='temperature_celsius',
                color_continuous_scale='RdYlBu_r'
            )
            fig_temp_bar.update_layout(height=400)
            st.plotly_chart(fig_temp_bar, use_container_width=True)
        
        with col2:
            # Humidity comparison
            fig_humidity = px.bar(
                latest_data.sort_values('humidity_percent'),
                x='city',
                y='humidity_percent',
                title="Current Humidity by City",
                labels={'humidity_percent': 'Humidity (%)'},
                color='humidity_percent',
                color_continuous_scale='Blues'
            )
            fig_humidity.update_layout(height=400)
            st.plotly_chart(fig_humidity, use_container_width=True)
    
    # Weather conditions distribution
    st.subheader("Weather Conditions")
    
    if 'weather_main' in weather_df.columns:
        weather_counts = weather_df['weather_main'].value_counts()
        
        fig_weather = px.pie(
            values=weather_counts.values,
            names=weather_counts.index,
            title="Weather Conditions Distribution"
        )
        fig_weather.update_layout(height=400)
        st.plotly_chart(fig_weather, use_container_width=True)
    
    # Wind analysis
    st.subheader("Wind Analysis")
    
    if not weather_df[['wind_speed_mps', 'wind_direction_deg']].empty:
        # Wind speed histogram
        fig_wind = px.histogram(
            weather_df,
            x='wind_speed_mps',
            nbins=20,
            title="Wind Speed Distribution",
            labels={'wind_speed_mps': 'Wind Speed (m/s)'}
        )
        fig_wind.update_layout(height=400)
        st.plotly_chart(fig_wind, use_container_width=True)
    
    # Data table
    st.header("📋 Recent Observations")
    
    # Select columns to display
    display_columns = [
        'city', 'country', 'observation_time', 'temperature_celsius', 
        'humidity_percent', 'pressure_hpa', 'weather_description',
        'wind_speed_mps'
    ]
    
    display_df = latest_data[display_columns].copy()
    display_df['observation_time'] = display_df['observation_time'].dt.strftime('%Y-%m-%d %H:%M')
    display_df = display_df.rename(columns={
        'temperature_celsius': 'Temp (°C)',
        'humidity_percent': 'Humidity (%)',
        'pressure_hpa': 'Pressure (hPa)',
        'weather_description': 'Conditions',
        'wind_speed_mps': 'Wind (m/s)',
        'observation_time': 'Last Updated'
    })
    
    st.dataframe(display_df, use_container_width=True)
    
    # Raw data section
    with st.expander("🔍 Raw Data Explorer"):
        st.subheader("Database Statistics")
        
        total_records = len(raw_df)
        valid_weather = len(weather_df)
        cities_count = len(weather_df['city'].unique()) if not weather_df.empty else 0
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Records", total_records)
        col2.metric("Valid Weather Records", valid_weather)
        col3.metric("Cities Monitored", cities_count)
        
        st.subheader("Sample Raw Data")
        if not raw_df.empty:
            sample_df = raw_df.head(10)[['city', 'observation_time', 'created_at']]
            st.dataframe(sample_df, use_container_width=True)
    
    # Footer
    st.markdown("---")
    st.markdown(
        "📡 Data refreshed every 5 minutes | "
        "🔄 Last updated: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

if __name__ == "__main__":
    main()