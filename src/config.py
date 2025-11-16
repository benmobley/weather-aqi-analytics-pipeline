"""
Configuration module for the Weather + Air Quality Analytics Pipeline.

This module handles loading configuration from environment variables and
provides default settings for the application.
"""

import os
from typing import List, Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Main configuration class for the application."""
    
    # Database Configuration
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_USER: str = os.getenv("DB_USER", "weather_user")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "weather_pass")
    DB_NAME: str = os.getenv("DB_NAME", "weather_db")
    
    # API Configuration
    OPENWEATHER_API_KEY: Optional[str] = os.getenv("OPENWEATHER_API_KEY")
    AIRNOW_API_KEY: Optional[str] = os.getenv("AIRNOW_API_KEY")
    
    # API Endpoints
    OPENWEATHER_BASE_URL: str = "https://api.openweathermap.org/data/2.5"
    AIRNOW_BASE_URL: str = "https://www.airnowapi.org/aq"
    
    # Default Cities to Monitor
    # Can be overridden by setting CITIES environment variable as comma-separated string
    DEFAULT_CITIES: List[str] = [
        "New York,US",
        "Los Angeles,US", 
        "Chicago,US",
        "Houston,US",
        "Phoenix,US",
        "Philadelphia,US",
        "San Antonio,US",
        "San Diego,US",
        "Dallas,US",
        "San Jose,US"
    ]
    
    @property
    def cities(self) -> List[str]:
        """Get list of cities from environment or use defaults."""
        cities_env = os.getenv("CITIES")
        if cities_env:
            return [city.strip() for city in cities_env.split(",")]
        return self.DEFAULT_CITIES
    
    @property
    def database_url(self) -> str:
        """Construct database URL from components."""
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    def validate_api_keys(self) -> None:
        """Validate that required API keys are present."""
        missing_keys = []
        
        if not self.OPENWEATHER_API_KEY:
            missing_keys.append("OPENWEATHER_API_KEY")
        
        if not self.AIRNOW_API_KEY:
            missing_keys.append("AIRNOW_API_KEY")
        
        if missing_keys:
            raise ValueError(
                f"Missing required API keys: {', '.join(missing_keys)}. "
                "Please set these in your .env file or environment variables."
            )
    
    def __repr__(self) -> str:
        """String representation of config (without sensitive data)."""
        return (
            f"Config("
            f"DB_HOST={self.DB_HOST}, "
            f"DB_PORT={self.DB_PORT}, "
            f"DB_NAME={self.DB_NAME}, "
            f"cities_count={len(self.cities)}, "
            f"has_openweather_key={bool(self.OPENWEATHER_API_KEY)}, "
            f"has_airnow_key={bool(self.AIRNOW_API_KEY)}"
            f")"
        )


# Global config instance
config = Config()


# Convenience functions
def get_database_url() -> str:
    """Get the database URL."""
    return config.database_url


def get_cities() -> List[str]:
    """Get the list of cities to monitor."""
    return config.cities


def validate_config() -> None:
    """Validate the current configuration."""
    config.validate_api_keys()
    print(f"Configuration loaded: {config}")


if __name__ == "__main__":
    # When run directly, print current configuration
    try:
        validate_config()
        print("✅ Configuration is valid!")
        print(f"Cities to monitor: {', '.join(get_cities())}")
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        exit(1)