{{
  config(
    materialized='table',
    docs={
      'node_color': 'lightgreen'
    }
  )
}}

/*
  Daily air quality facts table.
  
  This model aggregates air quality observations by city, date, and pollutant type
  to provide daily air quality summaries and health impact assessments.
*/

with air_quality_observations as (
    select * from {{ ref('stg_air_quality__observations') }}
),

daily_aqi_by_pollutant as (
    select
        city,
        country,
        date(observation_time) as observation_date,
        pollutant_type,
        
        -- AQI metrics
        round(avg(aqi_value), 0) as avg_aqi_value,
        min(aqi_value) as min_aqi_value,
        max(aqi_value) as max_aqi_value,
        
        -- Concentration metrics
        round(avg(concentration_value), 3) as avg_concentration_value,
        round(min(concentration_value), 3) as min_concentration_value,
        round(max(concentration_value), 3) as max_concentration_value,
        mode() within group (order by concentration_unit) as concentration_unit,
        
        -- Categories (most frequent)
        mode() within group (order by aqi_category_standard) as primary_aqi_category,
        mode() within group (order by aqi_color) as primary_aqi_color,
        round(avg(health_impact_level), 1) as avg_health_impact_level,
        
        -- Location information
        round(avg(latitude), 6) as avg_latitude,
        round(avg(longitude), 6) as avg_longitude,
        round(avg(distance_miles_approx), 2) as avg_distance_miles,
        
        -- Observation metadata
        count(*) as total_observations,
        min(observation_time) as first_observation_time,
        max(observation_time) as last_observation_time,
        
        -- Reporting area (most frequent)
        mode() within group (order by reporting_area) as primary_reporting_area,
        mode() within group (order by state_code) as primary_state_code
        
    from air_quality_observations
    where observation_time is not null
      and date(observation_time) is not null
      and aqi_value is not null
    group by city, country, date(observation_time), pollutant_type
),

daily_overall_aqi as (
    select
        city,
        country,
        observation_date,
        
        -- Overall AQI (worst pollutant determines overall AQI)
        max(avg_aqi_value) as overall_aqi_value,
        max(max_aqi_value) as peak_aqi_value,
        
        -- Determine primary pollutant (the one with highest AQI)
        (array_agg(
            pollutant_type order by avg_aqi_value desc
        ))[1] as primary_pollutant,
        
        -- Overall health category based on worst AQI
        case 
            when max(avg_aqi_value) between 0 and 50 then 'Good'
            when max(avg_aqi_value) between 51 and 100 then 'Moderate'
            when max(avg_aqi_value) between 101 and 150 then 'Unhealthy for Sensitive Groups'
            when max(avg_aqi_value) between 151 and 200 then 'Unhealthy'
            when max(avg_aqi_value) between 201 and 300 then 'Very Unhealthy'
            when max(avg_aqi_value) between 301 and 500 then 'Hazardous'
            else 'Unknown'
        end as overall_aqi_category,
        
        -- Overall color
        case 
            when max(avg_aqi_value) between 0 and 50 then 'Green'
            when max(avg_aqi_value) between 51 and 100 then 'Yellow'
            when max(avg_aqi_value) between 101 and 150 then 'Orange'
            when max(avg_aqi_value) between 151 and 200 then 'Red'
            when max(avg_aqi_value) between 201 and 300 then 'Purple'
            when max(avg_aqi_value) between 301 and 500 then 'Maroon'
            else 'Gray'
        end as overall_aqi_color,
        
        -- Health impact level
        case 
            when max(avg_aqi_value) between 0 and 50 then 1
            when max(avg_aqi_value) between 51 and 100 then 2
            when max(avg_aqi_value) between 101 and 150 then 3
            when max(avg_aqi_value) between 151 and 200 then 4
            when max(avg_aqi_value) between 201 and 300 then 5
            when max(avg_aqi_value) between 301 and 500 then 6
            else 0
        end as overall_health_impact_level,
        
        -- Pollutant diversity
        count(distinct pollutant_type) as pollutants_measured,
        array_agg(distinct pollutant_type order by pollutant_type) as pollutant_list,
        
        -- Aggregate location and metadata
        round(avg(avg_latitude), 6) as avg_latitude,
        round(avg(avg_longitude), 6) as avg_longitude,
        round(avg(avg_distance_miles), 2) as avg_distance_miles,
        sum(total_observations) as total_observations,
        min(first_observation_time) as first_observation_time,
        max(last_observation_time) as last_observation_time,
        mode() within group (order by primary_reporting_area) as primary_reporting_area,
        mode() within group (order by primary_state_code) as primary_state_code
        
    from daily_aqi_by_pollutant
    group by city, country, observation_date
),

aqi_with_trends as (
    select
        *,
        
        -- AQI trend compared to previous day
        lag(overall_aqi_value) over (
            partition by city, country 
            order by observation_date
        ) as prev_day_aqi_value,
        
        -- Calculate AQI change
        overall_aqi_value - lag(overall_aqi_value) over (
            partition by city, country 
            order by observation_date
        ) as aqi_change,
        
        -- 7-day rolling average
        round(avg(overall_aqi_value) over (
            partition by city, country 
            order by observation_date 
            rows between 6 preceding and current row
        ), 1) as aqi_7day_avg
        
    from daily_overall_aqi
),

final as (
    select
        -- Generate surrogate key
        {{ dbt_utils.generate_surrogate_key(['city', 'country', 'observation_date']) }} as daily_aqi_id,
        
        -- City information
        {{ dbt_utils.generate_surrogate_key(['city', 'country']) }} as city_id,
        city,
        country,
        observation_date,
        avg_latitude as latitude,
        avg_longitude as longitude,
        
        -- Overall AQI metrics
        overall_aqi_value,
        peak_aqi_value,
        overall_aqi_category,
        overall_aqi_color,
        overall_health_impact_level,
        primary_pollutant,
        
        -- Trend analysis
        aqi_change,
        aqi_7day_avg,
        case 
            when aqi_change > 20 then 'Significantly Worse'
            when aqi_change > 5 then 'Worse'
            when aqi_change between -5 and 5 then 'Stable'
            when aqi_change < -20 then 'Significantly Better'
            else 'Better'
        end as aqi_trend,
        
        -- Health recommendations
        case 
            when overall_aqi_value between 0 and 50 then 
                'Air quality is satisfactory. Outdoor activities are safe for everyone.'
            when overall_aqi_value between 51 and 100 then 
                'Air quality is acceptable. Unusually sensitive people should consider limiting prolonged outdoor exertion.'
            when overall_aqi_value between 101 and 150 then 
                'Sensitive groups should reduce prolonged or heavy outdoor exertion.'
            when overall_aqi_value between 151 and 200 then 
                'Everyone should reduce prolonged or heavy outdoor exertion.'
            when overall_aqi_value between 201 and 300 then 
                'Everyone should avoid prolonged or heavy outdoor exertion.'
            when overall_aqi_value between 301 and 500 then 
                'Everyone should avoid all outdoor exertion.'
            else 'Insufficient data for recommendations.'
        end as health_recommendation,
        
        -- Air quality assessment
        case 
            when overall_aqi_value <= 50 then 'Excellent'
            when overall_aqi_value <= 100 then 'Good'
            when overall_aqi_value <= 150 then 'Fair'
            when overall_aqi_value <= 200 then 'Poor'
            else 'Very Poor'
        end as air_quality_assessment,
        
        -- Pollutant information
        pollutants_measured,
        pollutant_list,
        
        -- Location and reporting
        primary_reporting_area,
        primary_state_code,
        avg_distance_miles,
        
        -- Observation metadata
        total_observations,
        first_observation_time,
        last_observation_time,
        
        -- Data quality flags
        case when total_observations >= 3 then true else false end as has_sufficient_observations,
        case when pollutants_measured >= 2 then true else false end as has_multiple_pollutants,
        case when avg_distance_miles <= 25 then true else false end as is_nearby_station,
        
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
        
    from aqi_with_trends
)

select * from final