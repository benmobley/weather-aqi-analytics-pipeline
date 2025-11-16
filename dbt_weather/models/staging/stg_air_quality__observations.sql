{{
  config(
    materialized='view',
    docs={
      'node_color': 'lightblue'
    }
  )
}}

/*
  Staging model for air quality observations.
  
  This model extracts and normalizes air quality data from the JSON format
  stored in the raw weather observations table.
*/

with raw_weather as (
    select * from {{ source('raw', 'weather_observations') }}
),

observations_with_aqi as (
    select 
        id,
        city,
        country,
        latitude,
        longitude,
        observation_time,
        created_at,
        air_quality_data
    from raw_weather
    where air_quality_data is not null 
      and air_quality_data != 'null'
      and air_quality_data::jsonb ? 'observations'
),

parsed_aqi as (
    select
        id,
        city,
        country,
        latitude,
        longitude,
        observation_time,
        created_at,
        
        -- Extract individual AQI observations from the JSON array
        jsonb_array_elements(air_quality_data::jsonb->'observations') as aqi_observation
        
    from observations_with_aqi
),

normalized_aqi as (
    select
        id as weather_observation_id,
        city,
        country,
        latitude,
        longitude,
        observation_time,
        created_at,
        
        -- AQI observation details
        (aqi_observation->>'DateObserved')::date as observation_date,
        (aqi_observation->>'HourObserved')::integer as observation_hour,
        (aqi_observation->>'LocalTimeZone')::varchar as local_timezone,
        (aqi_observation->>'ReportingArea')::varchar as reporting_area,
        (aqi_observation->>'StateCode')::varchar as state_code,
        
        -- Pollutant information
        (aqi_observation->>'ParameterName')::varchar as parameter_name,
        (aqi_observation->>'AQI')::integer as aqi_value,
        (aqi_observation->>'Value')::decimal(8,3) as concentration_value,
        (aqi_observation->>'Unit')::varchar as concentration_unit,
        (aqi_observation->>'Category')::varchar as aqi_category,
        
        -- Location details from AQI API
        (aqi_observation->>'Latitude')::decimal(10,8) as aqi_latitude,
        (aqi_observation->>'Longitude')::decimal(11,8) as aqi_longitude
        
    from parsed_aqi
),

final as (
    select
        weather_observation_id,
        city,
        country,
        latitude,
        longitude,
        observation_time,
        created_at,
        observation_date,
        observation_hour,
        local_timezone,
        reporting_area,
        state_code,
        
        -- Standardize parameter names
        case 
            when upper(parameter_name) like '%PM2.5%' then 'PM2.5'
            when upper(parameter_name) like '%PM10%' then 'PM10'
            when upper(parameter_name) like '%OZONE%' or upper(parameter_name) like '%O3%' then 'OZONE'
            when upper(parameter_name) like '%CO%' then 'CO'
            when upper(parameter_name) like '%NO2%' then 'NO2'
            when upper(parameter_name) like '%SO2%' then 'SO2'
            else parameter_name
        end as pollutant_type,
        
        parameter_name as original_parameter_name,
        
        -- Validate AQI values
        case 
            when aqi_value between {{ var('aqi_min') }} and {{ var('aqi_max') }}
            then aqi_value 
            else null 
        end as aqi_value,
        
        concentration_value,
        concentration_unit,
        
        -- Standardize AQI categories
        case 
            when aqi_value between 0 and 50 then 'Good'
            when aqi_value between 51 and 100 then 'Moderate'
            when aqi_value between 101 and 150 then 'Unhealthy for Sensitive Groups'
            when aqi_value between 151 and 200 then 'Unhealthy'
            when aqi_value between 201 and 300 then 'Very Unhealthy'
            when aqi_value between 301 and 500 then 'Hazardous'
            else 'Unknown'
        end as aqi_category_standard,
        
        aqi_category as aqi_category_original,
        
        -- Color coding for visualizations
        case 
            when aqi_value between 0 and 50 then 'Green'
            when aqi_value between 51 and 100 then 'Yellow'
            when aqi_value between 101 and 150 then 'Orange'
            when aqi_value between 151 and 200 then 'Red'
            when aqi_value between 201 and 300 then 'Purple'
            when aqi_value between 301 and 500 then 'Maroon'
            else 'Gray'
        end as aqi_color,
        
        -- Health impact level (numeric for ordering)
        case 
            when aqi_value between 0 and 50 then 1
            when aqi_value between 51 and 100 then 2
            when aqi_value between 101 and 150 then 3
            when aqi_value between 151 and 200 then 4
            when aqi_value between 201 and 300 then 5
            when aqi_value between 301 and 500 then 6
            else 0
        end as health_impact_level,
        
        aqi_latitude,
        aqi_longitude,
        
        -- Calculate distance between weather and AQI stations (approximate)
        case 
            when latitude is not null and longitude is not null 
                 and aqi_latitude is not null and aqi_longitude is not null
            then round(
                sqrt(
                    power((latitude - aqi_latitude) * 69, 2) + 
                    power((longitude - aqi_longitude) * 69 * cos(radians(latitude)), 2)
                )::numeric, 2
            )
            else null
        end as distance_miles_approx
        
    from normalized_aqi
    where aqi_value is not null
)

select * from final