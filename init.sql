-- Initialize database schemas and tables
-- This file is run when the PostgreSQL container starts

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Create raw weather observations table
CREATE TABLE IF NOT EXISTS raw.weather_observations (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    country VARCHAR(10),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    observation_time TIMESTAMP WITH TIME ZONE NOT NULL,
    weather_data JSONB NOT NULL,
    air_quality_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create unique constraint for upsert operations
ALTER TABLE raw.weather_observations ADD CONSTRAINT IF NOT EXISTS unique_city_observation_time 
    UNIQUE (city, observation_time);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_weather_city_time ON raw.weather_observations(city, observation_time);
CREATE INDEX IF NOT EXISTS idx_weather_observation_time ON raw.weather_observations(observation_time);
CREATE INDEX IF NOT EXISTS idx_weather_created_at ON raw.weather_observations(created_at);

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_weather_observations_updated_at 
    BEFORE UPDATE ON raw.weather_observations 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();