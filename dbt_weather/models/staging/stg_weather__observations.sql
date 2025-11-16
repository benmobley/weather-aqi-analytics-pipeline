{{
  config(
    materialized='view',
    docs={
      'node_color': 'lightblue'
    }
  )
}}

/*
  Staging model for weather observations.
  
  This model cleans and structures the raw weather data from the JSON format
  into a normalized table structure for easier querying and analysis.
*/

with raw_weather as (
    select * from {{ source('raw', 'weather_observations') }}
),

parsed_weather as (
    select
        id,
        city,
        country,
        latitude,
        longitude,
        observation_time,
        created_at,
        updated_at,
        
        -- Parse weather data JSON
        (weather_data::json->>'name')::varchar as weather_city_name,
        (weather_data::json->'coord'->>'lat')::decimal(10,8) as weather_latitude,
        (weather_data::json->'coord'->>'lon')::decimal(11,8) as weather_longitude,
        
        -- Main weather metrics
        (weather_data::json->'main'->>'temp')::decimal(5,2) as temperature_celsius,
        (weather_data::json->'main'->>'feels_like')::decimal(5,2) as feels_like_celsius,
        (weather_data::json->'main'->>'temp_min')::decimal(5,2) as temperature_min_celsius,
        (weather_data::json->'main'->>'temp_max')::decimal(5,2) as temperature_max_celsius,
        (weather_data::json->'main'->>'pressure')::integer as pressure_hpa,
        (weather_data::json->'main'->>'humidity')::integer as humidity_percent,
        
        -- Weather description
        (weather_data::json->'weather'->0->>'main')::varchar as weather_main,
        (weather_data::json->'weather'->0->>'description')::varchar as weather_description,
        (weather_data::json->'weather'->0->>'icon')::varchar as weather_icon,
        
        -- Wind information
        (weather_data::json->'wind'->>'speed')::decimal(5,2) as wind_speed_mps,
        (weather_data::json->'wind'->>'deg')::integer as wind_direction_degrees,
        
        -- Other metrics
        (weather_data::json->'clouds'->>'all')::integer as cloudiness_percent,
        (weather_data::json->>'visibility')::integer as visibility_meters,
        
        -- Timestamps
        to_timestamp((weather_data::json->>'dt')::bigint) as weather_timestamp,
        (weather_data::json->>'api_timestamp')::timestamp as api_timestamp,
        
        -- Air quality data (if available)
        case 
            when air_quality_data is not null and air_quality_data != 'null'
            then (air_quality_data::json->>'total_observations')::integer
            else null
        end as aqi_observations_count,
        
        -- Check for data quality issues
        case when weather_data::jsonb ? 'error' then weather_data::json->>'error' else null end as weather_error,
        case when air_quality_data::jsonb ? 'error' then air_quality_data::json->>'error' else null end as aqi_error,
        
        -- Original JSON for reference
        weather_data::json as weather_data_json,
        air_quality_data::json as air_quality_data_json
        
    from raw_weather
    where weather_data is not null
),

final as (
    select
        id,
        city,
        country,
        coalesce(latitude, weather_latitude) as latitude,
        coalesce(longitude, weather_longitude) as longitude,
        observation_time,
        created_at,
        updated_at,
        
        -- Weather metrics with data quality validations
        case 
            when temperature_celsius between {{ var('temperature_min') }} and {{ var('temperature_max') }}
            then temperature_celsius 
            else null 
        end as temperature_celsius,
        
        case 
            when feels_like_celsius between {{ var('temperature_min') }} and {{ var('temperature_max') }}
            then feels_like_celsius 
            else null 
        end as feels_like_celsius,
        
        temperature_min_celsius,
        temperature_max_celsius,
        pressure_hpa,
        
        case 
            when humidity_percent between {{ var('humidity_min') }} and {{ var('humidity_max') }}
            then humidity_percent 
            else null 
        end as humidity_percent,
        
        weather_main,
        weather_description,
        weather_icon,
        wind_speed_mps,
        wind_direction_degrees,
        cloudiness_percent,
        visibility_meters,
        weather_timestamp,
        api_timestamp,
        aqi_observations_count,
        
        -- Data quality flags
        weather_error is not null as has_weather_error,
        aqi_error is not null as has_aqi_error,
        weather_error,
        aqi_error,
        
        -- Convert temperatures to Fahrenheit for US users
        case 
            when temperature_celsius is not null 
            then round((temperature_celsius * 9.0/5.0) + 32, 1)
            else null 
        end as temperature_fahrenheit,
        
        case 
            when feels_like_celsius is not null 
            then round((feels_like_celsius * 9.0/5.0) + 32, 1)
            else null 
        end as feels_like_fahrenheit,
        
        -- Wind speed in mph
        case 
            when wind_speed_mps is not null 
            then round(wind_speed_mps * 2.237, 1)
            else null 
        end as wind_speed_mph,
        
        -- Categorize weather conditions
        case 
            when weather_main ilike '%clear%' then 'Clear'
            when weather_main ilike '%cloud%' then 'Cloudy'
            when weather_main ilike '%rain%' then 'Rainy'
            when weather_main ilike '%snow%' then 'Snowy'
            when weather_main ilike '%storm%' or weather_main ilike '%thunder%' then 'Stormy'
            when weather_main ilike '%mist%' or weather_main ilike '%fog%' then 'Misty'
            else 'Other'
        end as weather_category,
        
        -- Original JSON data
        weather_data_json,
        air_quality_data_json
        
    from parsed_weather
)

select * from final