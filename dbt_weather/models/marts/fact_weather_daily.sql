{{
  config(
    materialized='table',
    docs={
      'node_color': 'lightgreen'
    }
  )
}}

/*
  Daily weather facts table.
  
  This model aggregates weather observations by city and date to provide
  daily weather summaries including temperature, humidity, and weather conditions.
*/

with weather_observations as (
    select * from {{ ref('stg_weather__observations') }}
),

daily_weather as (
    select
        city,
        country,
        date(observation_time) as observation_date,
        
        -- Temperature metrics (Celsius)
        round(avg(temperature_celsius), 1) as avg_temperature_celsius,
        round(min(temperature_celsius), 1) as min_temperature_celsius,
        round(max(temperature_celsius), 1) as max_temperature_celsius,
        round(avg(feels_like_celsius), 1) as avg_feels_like_celsius,
        
        -- Temperature metrics (Fahrenheit)
        round(avg(temperature_fahrenheit), 1) as avg_temperature_fahrenheit,
        round(min(temperature_fahrenheit), 1) as min_temperature_fahrenheit,
        round(max(temperature_fahrenheit), 1) as max_temperature_fahrenheit,
        round(avg(feels_like_fahrenheit), 1) as avg_feels_like_fahrenheit,
        
        -- Humidity and pressure
        round(avg(humidity_percent), 0) as avg_humidity_percent,
        round(min(humidity_percent), 0) as min_humidity_percent,
        round(max(humidity_percent), 0) as max_humidity_percent,
        round(avg(pressure_hpa), 0) as avg_pressure_hpa,
        
        -- Wind metrics
        round(avg(wind_speed_mps), 1) as avg_wind_speed_mps,
        round(max(wind_speed_mps), 1) as max_wind_speed_mps,
        round(avg(wind_speed_mph), 1) as avg_wind_speed_mph,
        round(max(wind_speed_mph), 1) as max_wind_speed_mph,
        
        -- Cloud and visibility
        round(avg(cloudiness_percent), 0) as avg_cloudiness_percent,
        round(avg(visibility_meters), 0) as avg_visibility_meters,
        
        -- Weather conditions (most frequent)
        mode() within group (order by weather_main) as primary_weather_main,
        mode() within group (order by weather_description) as primary_weather_description,
        mode() within group (order by weather_category) as primary_weather_category,
        
        -- Observation counts
        count(*) as total_observations,
        count(case when has_weather_error then 1 end) as error_observations,
        count(case when not has_weather_error then 1 end) as successful_observations,
        
        -- Data quality
        round((count(case when not has_weather_error then 1 end)::decimal / count(*)) * 100, 1) as success_rate_percent,
        
        -- Time range of observations
        min(observation_time) as first_observation_time,
        max(observation_time) as last_observation_time,
        
        -- Coordinates (use most recent)
        (array_agg(latitude order by observation_time desc))[1] as latitude,
        (array_agg(longitude order by observation_time desc))[1] as longitude
        
    from weather_observations
    where observation_time is not null
      and date(observation_time) is not null
    group by city, country, date(observation_time)
),

weather_with_trends as (
    select
        *,
        
        -- Temperature trend compared to previous day
        lag(avg_temperature_celsius) over (
            partition by city, country 
            order by observation_date
        ) as prev_day_avg_temp_celsius,
        
        -- Calculate temperature change
        avg_temperature_celsius - lag(avg_temperature_celsius) over (
            partition by city, country 
            order by observation_date
        ) as temp_change_celsius,
        
        -- Weather stability (how many observations had the same primary weather)
        case 
            when total_observations = 1 then 100.0
            else round(
                (count(*) over (
                    partition by city, country, observation_date, primary_weather_main
                ) * 100.0) / total_observations, 1
            )
        end as weather_stability_percent
        
    from daily_weather
),

final as (
    select
        -- Generate surrogate key
        {{ dbt_utils.generate_surrogate_key(['city', 'country', 'observation_date']) }} as daily_weather_id,
        
        -- City information
        {{ dbt_utils.generate_surrogate_key(['city', 'country']) }} as city_id,
        city,
        country,
        observation_date,
        latitude,
        longitude,
        
        -- Temperature metrics
        avg_temperature_celsius,
        min_temperature_celsius,
        max_temperature_celsius,
        avg_feels_like_celsius,
        avg_temperature_fahrenheit,
        min_temperature_fahrenheit,
        max_temperature_fahrenheit,
        avg_feels_like_fahrenheit,
        
        -- Temperature analysis
        max_temperature_celsius - min_temperature_celsius as temperature_range_celsius,
        max_temperature_fahrenheit - min_temperature_fahrenheit as temperature_range_fahrenheit,
        temp_change_celsius,
        
        -- Temperature categories
        case 
            when avg_temperature_celsius < 0 then 'Freezing'
            when avg_temperature_celsius < 10 then 'Cold'
            when avg_temperature_celsius < 20 then 'Cool'
            when avg_temperature_celsius < 30 then 'Warm'
            else 'Hot'
        end as temperature_category,
        
        -- Humidity and atmospheric pressure
        avg_humidity_percent,
        min_humidity_percent,
        max_humidity_percent,
        avg_pressure_hpa,
        
        -- Humidity categories
        case 
            when avg_humidity_percent < 30 then 'Dry'
            when avg_humidity_percent < 60 then 'Comfortable'
            when avg_humidity_percent < 80 then 'Humid'
            else 'Very Humid'
        end as humidity_category,
        
        -- Wind information
        avg_wind_speed_mps,
        max_wind_speed_mps,
        avg_wind_speed_mph,
        max_wind_speed_mph,
        
        -- Wind categories (Beaufort scale approximation)
        case 
            when avg_wind_speed_mps < 0.5 then 'Calm'
            when avg_wind_speed_mps < 1.5 then 'Light Air'
            when avg_wind_speed_mps < 3.3 then 'Light Breeze'
            when avg_wind_speed_mps < 5.5 then 'Gentle Breeze'
            when avg_wind_speed_mps < 7.9 then 'Moderate Breeze'
            when avg_wind_speed_mps < 10.7 then 'Fresh Breeze'
            when avg_wind_speed_mps < 13.8 then 'Strong Breeze'
            else 'High Wind'
        end as wind_category,
        
        -- Sky and visibility
        avg_cloudiness_percent,
        avg_visibility_meters,
        
        -- Weather conditions
        primary_weather_main,
        primary_weather_description,
        primary_weather_category,
        weather_stability_percent,
        
        -- Observation metadata
        total_observations,
        successful_observations,
        error_observations,
        success_rate_percent,
        first_observation_time,
        last_observation_time,
        
        -- Data quality flags
        case when success_rate_percent >= 80 then true else false end as is_high_quality_data,
        case when total_observations >= 4 then true else false end as has_sufficient_observations,
        
        -- Seasonal classification (Northern Hemisphere)
        case 
            when extract(month from observation_date) in (12, 1, 2) then 'Winter'
            when extract(month from observation_date) in (3, 4, 5) then 'Spring'
            when extract(month from observation_date) in (6, 7, 8) then 'Summer'
            when extract(month from observation_date) in (9, 10, 11) then 'Fall'
        end as season,
        
        -- Day of week
        case extract(dow from observation_date)
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
        end as day_of_week,
        
        current_timestamp as created_at
        
    from weather_with_trends
)

select * from final