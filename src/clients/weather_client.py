"""
Weather client for OpenWeatherMap API (Free Tier).

This module provides functionality to fetch weather and air quality data
from the OpenWeatherMap free endpoints.
"""

import os
import logging
import requests
import time
from typing import Dict, Any, Optional, Tuple, Union
from datetime import datetime

logger = logging.getLogger(__name__)


class WeatherClient:
    """Client for fetching weather data from OpenWeatherMap free API endpoints."""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the weather client.
        
        Args:
            api_key: OpenWeatherMap API key. If None, uses OPENWEATHER_API_KEY env var
        """
        self.api_key = api_key or os.getenv('OPENWEATHER_API_KEY')
        self.base_url = "https://api.openweathermap.org/data/2.5"
        
        if not self.api_key:
            raise ValueError("OpenWeatherMap API key is required. Set OPENWEATHER_API_KEY environment variable.")
    
    def _make_request(self, url: str, params: Dict[str, Any], retries: int = 3) -> Dict[str, Any]:
        """Make API request with error handling and retries."""
        for attempt in range(retries):
            try:
                response = requests.get(url, params=params, timeout=30)
                
                # Handle specific HTTP errors
                if response.status_code == 401:
                    raise ValueError("Invalid API key (401 Unauthorized)")
                elif response.status_code == 404:
                    raise ValueError("Location not found (404)")
                elif response.status_code == 429:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(f"Rate limit hit, waiting {wait_time}s before retry {attempt + 1}/{retries}")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.Timeout:
                logger.warning(f"Request timeout, attempt {attempt + 1}/{retries}")
                if attempt == retries - 1:
                    raise
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {e}")
                if attempt == retries - 1:
                    raise
        
        raise requests.exceptions.RequestException("Max retries exceeded")

    def get_current_weather_by_city(self, city: str, country_code: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetch current weather data for a city.
        
        Args:
            city: City name (e.g., "Chicago")
            country_code: Optional country code (e.g., "US")
            
        Returns:
            Dictionary containing weather data
        """
        # Construct query string
        if country_code:
            query = f"{city},{country_code}"
        else:
            query = city
        
        # API parameters
        params = {
            'q': query,
            'appid': self.api_key,
            'units': 'metric'
        }
        
        url = f"{self.base_url}/weather"
        logger.info(f"Fetching weather for {query}")
        
        try:
            data = self._make_request(url, params)
            
            # Add metadata
            if data.get('cod') != 200:
                error_message = data.get('message', 'Unknown API error')
                raise ValueError(f"OpenWeatherMap API error: {error_message}")
            
            # Add metadata
            data['api_timestamp'] = datetime.utcnow().isoformat()
            data['query'] = query
            
            logger.info(f"Successfully fetched weather for {query}")
            return data
            
        except Exception as e:
            logger.error(f"Failed to fetch weather for {query}: {e}")
            return {
                'error': str(e),
                'api_timestamp': datetime.utcnow().isoformat(),
                'query': query
            }

    def get_current_weather_by_coords(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Fetch current weather data by coordinates.
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Dictionary containing weather data
        """
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.api_key,
            'units': 'metric'
        }
        
        url = f"{self.base_url}/weather"
        logger.info(f"Fetching weather for coordinates {lat}, {lon}")
        
        try:
            data = self._make_request(url, params)
            data['api_timestamp'] = datetime.utcnow().isoformat()
            data['query'] = f"{lat},{lon}"
            
            logger.info(f"Successfully fetched weather for coordinates {lat}, {lon}")
            return data
            
        except Exception as e:
            logger.error(f"Failed to fetch weather for coordinates {lat}, {lon}: {e}")
            return {
                'error': str(e),
                'api_timestamp': datetime.utcnow().isoformat(),
                'query': f"{lat},{lon}"
            }

    def get_air_pollution(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Fetch air pollution data by coordinates (free endpoint).
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Dictionary containing air pollution data
        """
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.api_key
        }
        
        url = f"{self.base_url}/air_pollution"
        logger.info(f"Fetching air pollution for coordinates {lat}, {lon}")
        
        try:
            data = self._make_request(url, params)
            data['api_timestamp'] = datetime.utcnow().isoformat()
            data['query'] = f"{lat},{lon}"
            
            logger.info(f"Successfully fetched air pollution for coordinates {lat}, {lon}")
            return data
            
        except Exception as e:
            logger.error(f"Failed to fetch air pollution for coordinates {lat}, {lon}: {e}")
            return {
                'error': str(e),
                'api_timestamp': datetime.utcnow().isoformat(),
                'query': f"{lat},{lon}"
            }

    def get_forecast(self, city: str, country_code: Optional[str] = None, cnt: int = 5) -> Dict[str, Any]:
        """
        Fetch 5-day weather forecast (free endpoint).
        
        Args:
            city: City name
            country_code: Optional country code
            cnt: Number of forecast points (max 40 for free tier)
            
        Returns:
            Dictionary containing forecast data
        """
        if country_code:
            query = f"{city},{country_code}"
        else:
            query = city
        
        params = {
            'q': query,
            'appid': self.api_key,
            'units': 'metric',
            'cnt': min(cnt, 40)  # Free tier limit
        }
        
        url = f"{self.base_url}/forecast"
        logger.info(f"Fetching forecast for {query}")
        
        try:
            data = self._make_request(url, params)
            data['api_timestamp'] = datetime.utcnow().isoformat()
            data['query'] = query
            
            logger.info(f"Successfully fetched forecast for {query}")
            return data
            
        except Exception as e:
            logger.error(f"Failed to fetch forecast for {query}: {e}")
            return {
                'error': str(e),
                'api_timestamp': datetime.utcnow().isoformat(),
                'query': query
            }

    def parse_city_country(self, city_input: str) -> Tuple[str, Optional[str]]:
        """Parse city input to extract city name and country code."""
        parts = city_input.split(',')
        city = parts[0].strip()
        country = parts[1].strip() if len(parts) > 1 else None
        return city, country

    def get_weather_for_cities(self, cities: list) -> Dict[str, Dict[str, Any]]:
        """Fetch weather data for multiple cities."""
        results = {}
        
        for city_input in cities:
            try:
                city, country = self.parse_city_country(city_input)
                weather_data = self.get_current_weather_by_city(city, country)
                results[city_input] = weather_data
                
            except Exception as e:
                logger.error(f"Failed to fetch weather for {city_input}: {e}")
                results[city_input] = {
                    'error': str(e),
                    'api_timestamp': datetime.utcnow().isoformat(),
                    'query': city_input
                }
        
        return results

    def extract_key_metrics(self, weather_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract key weather metrics from API response."""
        if 'error' in weather_data:
            return weather_data
        
        try:
            main = weather_data.get('main', {})
            weather = weather_data.get('weather', [{}])[0]
            wind = weather_data.get('wind', {})
            clouds = weather_data.get('clouds', {})
            
            return {
                'city': weather_data.get('name'),
                'country': weather_data.get('sys', {}).get('country'),
                'latitude': weather_data.get('coord', {}).get('lat'),
                'longitude': weather_data.get('coord', {}).get('lon'),
                'temperature': main.get('temp'),
                'feels_like': main.get('feels_like'),
                'temperature_min': main.get('temp_min'),
                'temperature_max': main.get('temp_max'),
                'pressure': main.get('pressure'),
                'humidity': main.get('humidity'),
                'weather_main': weather.get('main'),
                'weather_description': weather.get('description'),
                'cloudiness': clouds.get('all'),
                'wind_speed': wind.get('speed'),
                'wind_direction': wind.get('deg'),
                'visibility': weather_data.get('visibility'),
                'observation_time': datetime.utcfromtimestamp(weather_data.get('dt', 0)).isoformat(),
                'api_timestamp': weather_data.get('api_timestamp')
            }
            
        except Exception as e:
            logger.error(f"Failed to extract metrics from weather data: {e}")
            return {'error': f"Failed to parse weather data: {str(e)}"}


def main():
    """CLI interface for testing the weather client."""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python -m src.clients.weather_client <city_name>")
        print("Example: python -m src.clients.weather_client 'Chicago'")
        print("Example: python -m src.clients.weather_client 'New York,US'")
        sys.exit(1)
    
    city_name = sys.argv[1]
    
    try:
        print(f"🌤️  OpenWeatherMap Free Tier Client Test")
        print(f"Fetching weather for: {city_name}")
        print("-" * 50)
        
        client = WeatherClient()
        city, country = client.parse_city_country(city_name)
        
        # Get current weather
        weather_data = client.get_current_weather_by_city(city, country)
        
        if 'error' in weather_data:
            print(f"❌ Error: {weather_data['error']}")
            sys.exit(1)
        
        # Extract and display key metrics
        metrics = client.extract_key_metrics(weather_data)
        
        print(f"📍 Location: {metrics['city']}, {metrics['country']}")
        print(f"🌡️  Temperature: {metrics['temperature']}°C (feels like {metrics['feels_like']}°C)")
        print(f"☁️  Conditions: {metrics['weather_description'].title()}")
        print(f"💧 Humidity: {metrics['humidity']}%")
        print(f"🌬️  Wind: {metrics['wind_speed']} m/s")
        print(f"📊 Pressure: {metrics['pressure']} hPa")
        
        # Get coordinates for air pollution
        lat, lon = metrics['latitude'], metrics['longitude']
        if lat and lon:
            print(f"\n🏭 Air Quality (lat: {lat}, lon: {lon}):")
            air_data = client.get_air_pollution(lat, lon)
            
            if 'error' not in air_data and 'list' in air_data:
                aqi_info = air_data['list'][0]
                aqi = aqi_info.get('main', {}).get('aqi', 'N/A')
                components = aqi_info.get('components', {})
                
                aqi_levels = {1: "Good", 2: "Fair", 3: "Moderate", 4: "Poor", 5: "Very Poor"}
                print(f"   AQI Level: {aqi} ({aqi_levels.get(aqi, 'Unknown')})")
                
                if 'pm2_5' in components:
                    print(f"   PM2.5: {components['pm2_5']} μg/m³")
                if 'pm10' in components:
                    print(f"   PM10: {components['pm10']} μg/m³")
                if 'o3' in components:
                    print(f"   Ozone: {components['o3']} μg/m³")
            else:
                print(f"   ❌ Air quality data unavailable")
        
        print(f"\n✅ Data fetched successfully at {weather_data.get('api_timestamp', 'unknown time')}")
        
    except Exception as e:
        print(f"❌ Failed to fetch weather data: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()