"""
ETL module for fetching and loading weather and air quality data.

This module fetches weather and air quality data from APIs and loads it
into the PostgreSQL database for further processing.
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from src.config import config, get_cities
from src.db import get_raw_db_connection, init_db, test_connection
from src.clients.weather_client import WeatherClient
from src.clients.air_quality_client import AirQualityClient

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WeatherETL:
    """ETL class for weather and air quality data."""
    
    def __init__(self):
        """Initialize the ETL process."""
        self.weather_client = WeatherClient()
        self.air_quality_client = AirQualityClient()
        self.cities = get_cities()
    
    def fetch_weather_data(self) -> Dict[str, Dict[str, Any]]:
        """
        Fetch weather data for all configured cities.
        
        Returns:
            Dictionary mapping city names to weather data
        """
        logger.info(f"Fetching weather data for {len(self.cities)} cities")
        return self.weather_client.get_weather_for_cities(self.cities)
    
    def fetch_air_quality_data(self, weather_data: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Fetch air quality data using coordinates from weather data.
        
        Args:
            weather_data: Weather data containing coordinates
            
        Returns:
            Dictionary mapping city names to air quality data
        """
        logger.info("Fetching air quality data for cities with valid weather data")
        aqi_data = {}
        
        for city, weather in weather_data.items():
            if 'error' not in weather:
                logger.info(f"Fetching AQI for {city}")
                aqi_data[city] = self.air_quality_client.get_aqi_for_weather_data(weather)
            else:
                logger.warning(f"Skipping AQI for {city} due to weather data error")
                aqi_data[city] = {
                    'error': 'Weather data unavailable',
                    'api_timestamp': datetime.utcnow().isoformat()
                }
        
        return aqi_data
    
    def prepare_observation_record(self, city: str, weather_data: Dict[str, Any], 
                                 aqi_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare a database record from weather and air quality data.
        
        Args:
            city: City name
            weather_data: Weather data from API
            aqi_data: Air quality data from API
            
        Returns:
            Dictionary ready for database insertion
        """
        # Extract coordinates and basic info
        if 'error' not in weather_data:
            coord = weather_data.get('coord', {})
            lat = coord.get('lat')
            lon = coord.get('lon')
            city_name = weather_data.get('name', city.split(',')[0])
            country = weather_data.get('sys', {}).get('country')
            observation_time = datetime.utcfromtimestamp(weather_data.get('dt', 0))
        else:
            lat = lon = city_name = country = None
            observation_time = datetime.utcnow()
        
        # Prepare record
        record = {
            'city': city_name or city.split(',')[0],
            'country': country,
            'latitude': lat,
            'longitude': lon,
            'observation_time': observation_time,
            'weather_data': json.dumps(weather_data),
            'air_quality_data': json.dumps(aqi_data) if aqi_data else None
        }
        
        return record
    
    def load_observations_to_db(self, observations: List[Dict[str, Any]]) -> int:
        """
        Load observation records to the database.
        
        Args:
            observations: List of observation records
            
        Returns:
            Number of records inserted
        """
        if not observations:
            logger.warning("No observations to load")
            return 0
        
        insert_sql = """
        INSERT INTO raw.weather_observations 
        (city, country, latitude, longitude, observation_time, weather_data, air_quality_data)
        VALUES (%(city)s, %(country)s, %(latitude)s, %(longitude)s, %(observation_time)s, 
                %(weather_data)s, %(air_quality_data)s)
        ON CONFLICT (city, observation_time) 
        DO UPDATE SET
            country = EXCLUDED.country,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            weather_data = EXCLUDED.weather_data,
            air_quality_data = EXCLUDED.air_quality_data,
            updated_at = CURRENT_TIMESTAMP
        """
        
        try:
            with get_raw_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(insert_sql, observations)
                    rows_affected = cursor.rowcount
                    
            logger.info(f"Successfully loaded {rows_affected} observations to database")
            return rows_affected
            
        except Exception as e:
            logger.error(f"Failed to load observations to database: {e}")
            raise
    
    def run_etl(self) -> Dict[str, Any]:
        """
        Run the complete ETL process.
        
        Returns:
            Summary of the ETL run
        """
        start_time = datetime.utcnow()
        logger.info("Starting Weather + AQI ETL process")
        
        try:
            # Fetch weather data
            weather_data = self.fetch_weather_data()
            
            # Fetch air quality data
            aqi_data = self.fetch_air_quality_data(weather_data)
            
            # Prepare observations for database
            observations = []
            for city in self.cities:
                weather = weather_data.get(city, {})
                aqi = aqi_data.get(city, {})
                
                record = self.prepare_observation_record(city, weather, aqi)
                observations.append(record)
            
            # Load to database
            records_loaded = self.load_observations_to_db(observations)
            
            # Prepare summary
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            summary = {
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'cities_processed': len(self.cities),
                'records_loaded': records_loaded,
                'weather_successes': len([w for w in weather_data.values() if 'error' not in w]),
                'weather_errors': len([w for w in weather_data.values() if 'error' in w]),
                'aqi_successes': len([a for a in aqi_data.values() if 'error' not in a]),
                'aqi_errors': len([a for a in aqi_data.values() if 'error' in a]),
                'status': 'success'
            }
            
            logger.info(f"ETL process completed successfully in {duration:.2f} seconds")
            logger.info(f"Processed {summary['cities_processed']} cities, loaded {summary['records_loaded']} records")
            
            return summary
            
        except Exception as e:
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            logger.error(f"ETL process failed after {duration:.2f} seconds: {e}")
            
            return {
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'status': 'error',
                'error': str(e)
            }
    
    def get_recent_observations(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent observations from the database.
        
        Args:
            limit: Maximum number of records to return
            
        Returns:
            List of recent observation records
        """
        sql = """
        SELECT id, city, country, latitude, longitude, observation_time, 
               created_at, updated_at
        FROM raw.weather_observations
        ORDER BY created_at DESC
        LIMIT %s
        """
        
        try:
            with get_raw_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (limit,))
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    
                    return [dict(zip(columns, row)) for row in rows]
                    
        except Exception as e:
            logger.error(f"Failed to fetch recent observations: {e}")
            return []


def main():
    """Main function to run the ETL process."""
    print("🌤️  Weather + Air Quality ETL Pipeline")
    print(f"   Cities: {', '.join(get_cities()[:3])}{'...' if len(get_cities()) > 3 else ''}")
    print(f"   Database: {config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}")
    print()
    
    try:
        # Validate configuration
        config.validate_api_keys()
        
        # Test database connection
        if not test_connection():
            logger.error("Database connection failed")
            return
        
        # Initialize database if needed
        init_db()
        
        # Run ETL
        etl = WeatherETL()
        summary = etl.run_etl()
        
        # Print summary
        if summary['status'] == 'success':
            print("✅ ETL Process Summary:")
            print(f"   Duration: {summary['duration_seconds']:.2f} seconds")
            print(f"   Cities processed: {summary['cities_processed']}")
            print(f"   Records loaded: {summary['records_loaded']}")
            print(f"   Weather API successes: {summary['weather_successes']}")
            print(f"   Air Quality API successes: {summary['aqi_successes']}")
            
            # Show recent observations
            print("\n📊 Recent Observations:")
            recent = etl.get_recent_observations(5)
            for obs in recent:
                print(f"   {obs['city']}: {obs['observation_time']} ({obs['created_at']})")
        else:
            print(f"❌ ETL Process Failed: {summary['error']}")
            
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()