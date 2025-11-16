"""
Air Quality client for AirNow API.

This module provides functionality to fetch air quality data
from the AirNow API for specified cities and coordinates.
"""

import logging
import requests
from typing import Dict, Any, Optional, List
from datetime import datetime

from src.config import config

logger = logging.getLogger(__name__)


class AirQualityClient:
    """Client for fetching air quality data from AirNow API."""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the air quality client.
        
        Args:
            api_key: AirNow API key. If None, uses config.AIRNOW_API_KEY
        """
        self.api_key = api_key or config.AIRNOW_API_KEY
        self.base_url = config.AIRNOW_BASE_URL
        
        if not self.api_key:
            logger.warning("AirNow API key not provided. Air quality data will not be available.")
    
    def get_current_aqi_by_coordinates(self, latitude: float, longitude: float) -> Dict[str, Any]:
        """
        Fetch current air quality data by coordinates.
        
        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            
        Returns:
            Dictionary containing air quality data
            
        Raises:
            requests.RequestException: If API request fails
            ValueError: If API returns error response
        """
        if not self.api_key:
            return {
                'error': 'AirNow API key not configured',
                'api_timestamp': datetime.utcnow().isoformat(),
                'coordinates': {'lat': latitude, 'lon': longitude}
            }
        
        # API parameters
        params = {
            'format': 'application/json',
            'latitude': latitude,
            'longitude': longitude,
            'distance': 50,  # Search within 50 miles
            'API_KEY': self.api_key
        }
        
        url = f"{self.base_url}/observation/latLong/current"
        logger.info(f"Fetching AQI for coordinates {latitude}, {longitude}")
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Add metadata
            if isinstance(data, list):
                result = {
                    'observations': data,
                    'api_timestamp': datetime.utcnow().isoformat(),
                    'coordinates': {'lat': latitude, 'lon': longitude},
                    'total_observations': len(data)
                }
            else:
                result = data
                result['api_timestamp'] = datetime.utcnow().isoformat()
                result['coordinates'] = {'lat': latitude, 'lon': longitude}
            
            logger.info(f"Successfully fetched AQI for coordinates {latitude}, {longitude}")
            return result
            
        except requests.RequestException as e:
            logger.error(f"Failed to fetch AQI for coordinates {latitude}, {longitude}: {e}")
            return {
                'error': str(e),
                'api_timestamp': datetime.utcnow().isoformat(),
                'coordinates': {'lat': latitude, 'lon': longitude}
            }
        except Exception as e:
            logger.error(f"Unexpected error fetching AQI: {e}")
            return {
                'error': f"Unexpected error: {str(e)}",
                'api_timestamp': datetime.utcnow().isoformat(),
                'coordinates': {'lat': latitude, 'lon': longitude}
            }
    
    def get_current_aqi_by_zip(self, zip_code: str, country_code: str = "US") -> Dict[str, Any]:
        """
        Fetch current air quality data by ZIP code.
        
        Args:
            zip_code: ZIP code
            country_code: Country code (default: "US")
            
        Returns:
            Dictionary containing air quality data
        """
        if not self.api_key:
            return {
                'error': 'AirNow API key not configured',
                'api_timestamp': datetime.utcnow().isoformat(),
                'zip_code': zip_code
            }
        
        params = {
            'format': 'application/json',
            'zipCode': zip_code,
            'distance': 50,
            'API_KEY': self.api_key
        }
        
        url = f"{self.base_url}/observation/zipCode/current"
        logger.info(f"Fetching AQI for ZIP code {zip_code}")
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Add metadata
            if isinstance(data, list):
                result = {
                    'observations': data,
                    'api_timestamp': datetime.utcnow().isoformat(),
                    'zip_code': zip_code,
                    'country_code': country_code,
                    'total_observations': len(data)
                }
            else:
                result = data
                result['api_timestamp'] = datetime.utcnow().isoformat()
                result['zip_code'] = zip_code
                result['country_code'] = country_code
            
            logger.info(f"Successfully fetched AQI for ZIP code {zip_code}")
            return result
            
        except requests.RequestException as e:
            logger.error(f"Failed to fetch AQI for ZIP code {zip_code}: {e}")
            return {
                'error': str(e),
                'api_timestamp': datetime.utcnow().isoformat(),
                'zip_code': zip_code
            }
    
    def extract_key_metrics(self, aqi_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract key air quality metrics from API response.
        
        Args:
            aqi_data: Raw air quality data from API
            
        Returns:
            Dictionary with extracted metrics
        """
        if 'error' in aqi_data:
            return aqi_data
        
        try:
            observations = aqi_data.get('observations', [])
            if not observations:
                return {
                    'error': 'No air quality observations found',
                    'api_timestamp': aqi_data.get('api_timestamp')
                }
            
            # Find PM2.5 and Ozone observations
            pm25_obs = None
            ozone_obs = None
            
            for obs in observations:
                parameter = obs.get('ParameterName', '').upper()
                if 'PM2.5' in parameter and not pm25_obs:
                    pm25_obs = obs
                elif 'OZONE' in parameter and not ozone_obs:
                    ozone_obs = obs
            
            # Extract overall AQI (usually the highest of all parameters)
            overall_aqi = max([obs.get('AQI', 0) for obs in observations])
            
            # Determine overall category
            category_mapping = {
                range(0, 51): ('Good', 'Green'),
                range(51, 101): ('Moderate', 'Yellow'),
                range(101, 151): ('Unhealthy for Sensitive Groups', 'Orange'),
                range(151, 201): ('Unhealthy', 'Red'),
                range(201, 301): ('Very Unhealthy', 'Purple'),
                range(301, 501): ('Hazardous', 'Maroon')
            }
            
            overall_category = 'Unknown'
            overall_color = 'Gray'
            for aqi_range, (category, color) in category_mapping.items():
                if overall_aqi in aqi_range:
                    overall_category = category
                    overall_color = color
                    break
            
            return {
                'overall_aqi': overall_aqi,
                'overall_category': overall_category,
                'overall_color': overall_color,
                'pm25_aqi': pm25_obs.get('AQI') if pm25_obs else None,
                'pm25_value': pm25_obs.get('Value') if pm25_obs else None,
                'pm25_unit': pm25_obs.get('Unit') if pm25_obs else None,
                'ozone_aqi': ozone_obs.get('AQI') if ozone_obs else None,
                'ozone_value': ozone_obs.get('Value') if ozone_obs else None,
                'ozone_unit': ozone_obs.get('Unit') if ozone_obs else None,
                'reporting_area': observations[0].get('ReportingArea') if observations else None,
                'state_code': observations[0].get('StateCode') if observations else None,
                'observation_date': observations[0].get('DateObserved') if observations else None,
                'observation_hour': observations[0].get('HourObserved') if observations else None,
                'total_observations': len(observations),
                'api_timestamp': aqi_data.get('api_timestamp')
            }
            
        except Exception as e:
            logger.error(f"Failed to extract metrics from AQI data: {e}")
            return {'error': f"Failed to parse AQI data: {str(e)}"}
    
    def get_aqi_for_weather_data(self, weather_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get air quality data using coordinates from weather data.
        
        Args:
            weather_data: Weather data containing coordinates
            
        Returns:
            Dictionary containing air quality data
        """
        if 'error' in weather_data:
            return {
                'error': 'Cannot fetch AQI: Weather data contains error',
                'api_timestamp': datetime.utcnow().isoformat()
            }
        
        try:
            coord = weather_data.get('coord', {})
            lat = coord.get('lat')
            lon = coord.get('lon')
            
            if lat is None or lon is None:
                return {
                    'error': 'Weather data missing coordinates',
                    'api_timestamp': datetime.utcnow().isoformat()
                }
            
            return self.get_current_aqi_by_coordinates(lat, lon)
            
        except Exception as e:
            logger.error(f"Failed to get AQI from weather data: {e}")
            return {
                'error': f"Failed to process weather data: {str(e)}",
                'api_timestamp': datetime.utcnow().isoformat()
            }


def test_air_quality_client():
    """Test function for the air quality client."""
    try:
        client = AirQualityClient()
        
        print("Testing air quality client...")
        
        # Test with coordinates (New York City)
        nyc_lat, nyc_lon = 40.7128, -74.0060
        aqi_data = client.get_current_aqi_by_coordinates(nyc_lat, nyc_lon)
        
        if 'error' in aqi_data:
            print(f"❌ Coordinates test: {aqi_data['error']}")
        else:
            metrics = client.extract_key_metrics(aqi_data)
            aqi = metrics.get('overall_aqi')
            category = metrics.get('overall_category')
            print(f"✅ NYC AQI: {aqi} ({category})")
        
        # Test with ZIP code
        zip_data = client.get_current_aqi_by_zip("10001")
        
        if 'error' in zip_data:
            print(f"⚠️  ZIP test: {zip_data['error']}")
        else:
            metrics = client.extract_key_metrics(zip_data)
            aqi = metrics.get('overall_aqi')
            category = metrics.get('overall_category')
            print(f"✅ ZIP 10001 AQI: {aqi} ({category})")
                
    except Exception as e:
        print(f"❌ Air quality client test failed: {e}")


if __name__ == "__main__":
    test_air_quality_client()