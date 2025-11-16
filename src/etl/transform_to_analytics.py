"""
Transform module for additional data processing before dbt.

This module provides optional Python-based transformations that can be
applied to the raw data before dbt processing, such as data validation,
enrichment, or complex calculations.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from src.db import get_raw_db_connection, execute_sql
from src.config import config

logger = logging.getLogger(__name__)


class WeatherDataTransformer:
    """Class for performing Python-based transformations on weather data."""
    
    def __init__(self):
        """Initialize the transformer."""
        pass
    
    def validate_weather_data(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and clean weather data record.
        
        Args:
            record: Raw weather observation record
            
        Returns:
            Validation results and cleaned data
        """
        validation_results = {
            'record_id': record.get('id'),
            'city': record.get('city'),
            'is_valid': True,
            'issues': [],
            'cleaned_weather_data': None,
            'cleaned_aqi_data': None
        }
        
        try:
            # Parse JSON data
            weather_data = json.loads(record.get('weather_data', '{}'))
            aqi_data = json.loads(record.get('air_quality_data', '{}')) if record.get('air_quality_data') else {}
            
            # Validate weather data
            if 'error' in weather_data:
                validation_results['issues'].append(f"Weather API error: {weather_data['error']}")
                validation_results['is_valid'] = False
            else:
                # Check required weather fields
                required_fields = ['main', 'weather', 'coord']
                for field in required_fields:
                    if field not in weather_data:
                        validation_results['issues'].append(f"Missing weather field: {field}")
                        validation_results['is_valid'] = False
                
                # Validate temperature ranges
                main_data = weather_data.get('main', {})
                temp = main_data.get('temp')
                if temp is not None:
                    if temp < -100 or temp > 70:  # Celsius
                        validation_results['issues'].append(f"Temperature out of range: {temp}°C")
                
                # Validate humidity
                humidity = main_data.get('humidity')
                if humidity is not None:
                    if humidity < 0 or humidity > 100:
                        validation_results['issues'].append(f"Humidity out of range: {humidity}%")
            
            # Validate AQI data
            if aqi_data and 'error' not in aqi_data:
                observations = aqi_data.get('observations', [])
                if observations:
                    for obs in observations:
                        aqi_value = obs.get('AQI')
                        if aqi_value is not None and (aqi_value < 0 or aqi_value > 500):
                            validation_results['issues'].append(f"AQI out of range: {aqi_value}")
            
            # Clean data (example transformations)
            validation_results['cleaned_weather_data'] = self._clean_weather_data(weather_data)
            validation_results['cleaned_aqi_data'] = self._clean_aqi_data(aqi_data)
            
        except json.JSONDecodeError as e:
            validation_results['issues'].append(f"JSON parsing error: {e}")
            validation_results['is_valid'] = False
        except Exception as e:
            validation_results['issues'].append(f"Validation error: {e}")
            validation_results['is_valid'] = False
        
        return validation_results
    
    def _clean_weather_data(self, weather_data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and standardize weather data."""
        if 'error' in weather_data:
            return weather_data
        
        cleaned = weather_data.copy()
        
        # Round numeric values to reasonable precision
        main_data = cleaned.get('main', {})
        for field in ['temp', 'feels_like', 'temp_min', 'temp_max']:
            if field in main_data and main_data[field] is not None:
                main_data[field] = round(main_data[field], 1)
        
        if 'pressure' in main_data and main_data['pressure'] is not None:
            main_data['pressure'] = round(main_data['pressure'])
        
        # Clean wind data
        wind_data = cleaned.get('wind', {})
        if 'speed' in wind_data and wind_data['speed'] is not None:
            wind_data['speed'] = round(wind_data['speed'], 1)
        
        if 'deg' in wind_data and wind_data['deg'] is not None:
            wind_data['deg'] = round(wind_data['deg'])
        
        return cleaned
    
    def _clean_aqi_data(self, aqi_data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and standardize air quality data."""
        if 'error' in aqi_data or not aqi_data:
            return aqi_data
        
        cleaned = aqi_data.copy()
        
        # Clean observations
        observations = cleaned.get('observations', [])
        cleaned_observations = []
        
        for obs in observations:
            cleaned_obs = obs.copy()
            
            # Round AQI values
            if 'AQI' in cleaned_obs and cleaned_obs['AQI'] is not None:
                cleaned_obs['AQI'] = round(cleaned_obs['AQI'])
            
            # Round concentration values
            if 'Value' in cleaned_obs and cleaned_obs['Value'] is not None:
                cleaned_obs['Value'] = round(cleaned_obs['Value'], 3)
            
            cleaned_observations.append(cleaned_obs)
        
        cleaned['observations'] = cleaned_observations
        return cleaned
    
    def calculate_weather_trends(self, city: str, days: int = 7) -> Dict[str, Any]:
        """
        Calculate weather trends for a city over the specified period.
        
        Args:
            city: City name
            days: Number of days to analyze
            
        Returns:
            Dictionary containing trend analysis
        """
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        sql = """
        SELECT observation_time, weather_data, air_quality_data
        FROM raw.weather_observations
        WHERE city = %s AND observation_time >= %s
        ORDER BY observation_time
        """
        
        try:
            with get_raw_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (city, cutoff_date))
                    rows = cursor.fetchall()
            
            if not rows:
                return {'error': f'No data found for {city} in the last {days} days'}
            
            temperatures = []
            humidity_values = []
            aqi_values = []
            
            for row in rows:
                try:
                    weather_data = json.loads(row[1])
                    aqi_data = json.loads(row[2]) if row[2] else {}
                    
                    # Extract temperature
                    temp = weather_data.get('main', {}).get('temp')
                    if temp is not None:
                        temperatures.append(temp)
                    
                    # Extract humidity
                    humidity = weather_data.get('main', {}).get('humidity')
                    if humidity is not None:
                        humidity_values.append(humidity)
                    
                    # Extract AQI
                    if aqi_data and 'observations' in aqi_data:
                        aqi_list = [obs.get('AQI') for obs in aqi_data['observations'] if obs.get('AQI')]
                        if aqi_list:
                            aqi_values.append(max(aqi_list))
                
                except json.JSONDecodeError:
                    continue
            
            # Calculate trends
            trends = {
                'city': city,
                'period_days': days,
                'total_observations': len(rows),
                'temperature_trend': self._calculate_trend(temperatures),
                'humidity_trend': self._calculate_trend(humidity_values),
                'aqi_trend': self._calculate_trend(aqi_values),
                'calculated_at': datetime.utcnow().isoformat()
            }
            
            return trends
            
        except Exception as e:
            logger.error(f"Failed to calculate trends for {city}: {e}")
            return {'error': str(e)}
    
    def _calculate_trend(self, values: List[float]) -> Dict[str, Any]:
        """Calculate trend statistics for a list of values."""
        if not values:
            return {'error': 'No values provided'}
        
        try:
            import statistics
            
            # Basic statistics
            avg = statistics.mean(values)
            median = statistics.median(values)
            min_val = min(values)
            max_val = max(values)
            
            # Standard deviation
            std_dev = statistics.stdev(values) if len(values) > 1 else 0
            
            # Simple trend calculation (comparing first half to second half)
            mid_point = len(values) // 2
            first_half_avg = statistics.mean(values[:mid_point]) if mid_point > 0 else avg
            second_half_avg = statistics.mean(values[mid_point:]) if mid_point < len(values) else avg
            
            trend_direction = 'stable'
            trend_magnitude = abs(second_half_avg - first_half_avg)
            
            if second_half_avg > first_half_avg + std_dev * 0.5:
                trend_direction = 'increasing'
            elif second_half_avg < first_half_avg - std_dev * 0.5:
                trend_direction = 'decreasing'
            
            return {
                'count': len(values),
                'average': round(avg, 2),
                'median': round(median, 2),
                'min': round(min_val, 2),
                'max': round(max_val, 2),
                'std_dev': round(std_dev, 2),
                'trend_direction': trend_direction,
                'trend_magnitude': round(trend_magnitude, 2)
            }
            
        except Exception as e:
            return {'error': f'Trend calculation failed: {str(e)}'}
    
    def run_data_quality_check(self, limit: int = 100) -> Dict[str, Any]:
        """
        Run data quality checks on recent observations.
        
        Args:
            limit: Number of recent records to check
            
        Returns:
            Data quality report
        """
        sql = """
        SELECT id, city, observation_time, weather_data, air_quality_data
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
                    records = [dict(zip(columns, row)) for row in rows]
            
            total_records = len(records)
            valid_records = 0
            all_issues = []
            
            for record in records:
                validation = self.validate_weather_data(record)
                if validation['is_valid']:
                    valid_records += 1
                else:
                    all_issues.extend(validation['issues'])
            
            # Categorize issues
            issue_categories = {}
            for issue in all_issues:
                category = issue.split(':')[0] if ':' in issue else 'Other'
                issue_categories[category] = issue_categories.get(category, 0) + 1
            
            return {
                'total_records_checked': total_records,
                'valid_records': valid_records,
                'invalid_records': total_records - valid_records,
                'data_quality_score': round((valid_records / total_records) * 100, 2) if total_records > 0 else 0,
                'issue_categories': issue_categories,
                'checked_at': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Data quality check failed: {e}")
            return {'error': str(e)}


def main():
    """Main function for running transformations."""
    print("🔄 Weather Data Transformer")
    
    try:
        transformer = WeatherDataTransformer()
        
        # Run data quality check
        print("\nRunning data quality check...")
        quality_report = transformer.run_data_quality_check()
        
        if 'error' not in quality_report:
            print(f"✅ Data Quality Report:")
            print(f"   Records checked: {quality_report['total_records_checked']}")
            print(f"   Valid records: {quality_report['valid_records']}")
            print(f"   Quality score: {quality_report['data_quality_score']}%")
            
            if quality_report['issue_categories']:
                print("   Issue categories:")
                for category, count in quality_report['issue_categories'].items():
                    print(f"     {category}: {count}")
        else:
            print(f"❌ Quality check failed: {quality_report['error']}")
        
        # Example: Calculate trends for a city
        cities = config.cities
        if cities:
            city = cities[0].split(',')[0]  # Take first city
            print(f"\nCalculating trends for {city}...")
            trends = transformer.calculate_weather_trends(city, days=7)
            
            if 'error' not in trends:
                print(f"📈 Trends for {city}:")
                temp_trend = trends.get('temperature_trend', {})
                if 'error' not in temp_trend:
                    print(f"   Temperature: {temp_trend['average']}°C avg, trend: {temp_trend['trend_direction']}")
                
                aqi_trend = trends.get('aqi_trend', {})
                if 'error' not in aqi_trend:
                    print(f"   AQI: {aqi_trend['average']} avg, trend: {aqi_trend['trend_direction']}")
            else:
                print(f"❌ Trend calculation failed: {trends['error']}")
        
    except Exception as e:
        print(f"❌ Transformation failed: {e}")


if __name__ == "__main__":
    main()