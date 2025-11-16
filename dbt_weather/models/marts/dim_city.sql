{{
  config(
    materialized='table',
    docs={
      'node_color': 'lightgreen'
    }
  )
}}

/*
  Dimension table for cities.
  
  This model creates a master list of cities with their geographic and 
  metadata information derived from weather observations.
*/

with weather_observations as (
    select * from {{ ref('stg_weather__observations') }}
),

city_info as (
    select
        city,
        country,
        latitude,
        longitude,
        min(observation_time) as first_observation,
        max(observation_time) as last_observation,
        count(*) as total_observations,
        count(case when has_weather_error then 1 end) as error_count,
        count(case when not has_weather_error then 1 end) as success_count
    from weather_observations
    where city is not null
    group by city, country, latitude, longitude
),

city_stats as (
    select
        city,
        country,
        latitude,
        longitude,
        first_observation,
        last_observation,
        total_observations,
        error_count,
        success_count,
        
        -- Data quality metrics
        round((success_count::decimal / total_observations) * 100, 2) as success_rate_percent,
        
        -- Geographic classifications
        case 
            when latitude > 66.5 then 'Arctic'
            when latitude > 23.5 then 'Northern Temperate'
            when latitude > -23.5 then 'Tropical'
            when latitude > -66.5 then 'Southern Temperate'
            else 'Antarctic'
        end as climate_zone,
        
        case 
            when country = 'US' then
                case 
                    when longitude > -75 then 'Eastern US'
                    when longitude > -105 then 'Central US'
                    else 'Western US'
                end
            else 'International'
        end as region_classification,
        
        -- Time zone estimation (rough)
        case 
            when longitude between -180 and -165 then 'UTC-11'
            when longitude between -165 and -150 then 'UTC-10'
            when longitude between -150 and -135 then 'UTC-9'
            when longitude between -135 and -120 then 'UTC-8'
            when longitude between -120 and -105 then 'UTC-7'
            when longitude between -105 and -90 then 'UTC-6'
            when longitude between -90 and -75 then 'UTC-5'
            when longitude between -75 and -60 then 'UTC-4'
            when longitude between -60 and -45 then 'UTC-3'
            when longitude between -45 and -30 then 'UTC-2'
            when longitude between -30 and -15 then 'UTC-1'
            when longitude between -15 and 0 then 'UTC+0'
            when longitude between 0 and 15 then 'UTC+1'
            when longitude between 15 and 30 then 'UTC+2'
            when longitude between 30 and 45 then 'UTC+3'
            when longitude between 45 and 60 then 'UTC+4'
            when longitude between 60 and 75 then 'UTC+5'
            when longitude between 75 and 90 then 'UTC+6'
            when longitude between 90 and 105 then 'UTC+7'
            when longitude between 105 and 120 then 'UTC+8'
            when longitude between 120 and 135 then 'UTC+9'
            when longitude between 135 and 150 then 'UTC+10'
            when longitude between 150 and 165 then 'UTC+11'
            when longitude between 165 and 180 then 'UTC+12'
            else 'Unknown'
        end as estimated_timezone
    from city_info
),

final as (
    select
        -- Generate a unique city ID
        {{ dbt_utils.generate_surrogate_key(['city', 'country']) }} as city_id,
        
        city,
        country,
        latitude,
        longitude,
        climate_zone,
        region_classification,
        estimated_timezone,
        
        -- Observation metadata
        first_observation,
        last_observation,
        total_observations,
        success_count,
        error_count,
        success_rate_percent,
        
        -- Data freshness indicators
        case 
            when last_observation >= current_date - interval '1 day' then 'Fresh'
            when last_observation >= current_date - interval '7 days' then 'Recent'
            when last_observation >= current_date - interval '30 days' then 'Stale'
            else 'Very Stale'
        end as data_freshness,
        
        -- Active status
        case 
            when last_observation >= current_date - interval '7 days' 
                 and success_rate_percent >= 70 
            then true 
            else false 
        end as is_active,
        
        -- Data quality tier
        case 
            when success_rate_percent >= 95 then 'Excellent'
            when success_rate_percent >= 85 then 'Good'
            when success_rate_percent >= 70 then 'Fair'
            else 'Poor'
        end as data_quality_tier,
        
        current_timestamp as created_at
        
    from city_stats
)

select * from final