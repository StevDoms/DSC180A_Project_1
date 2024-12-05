import pandas as pd

def calculate_psps_probability(merged_data, gis_weather_station, condition):
    """Calculates the PSPS probability for each weather station using record-specific thresholds
       and merges it with GIS weather station metadata.

    Args:
        merged_data (pd.DataFrame): The merged dataset containing wind speed data and alert thresholds.
        gis_weather_station (pd.DataFrame): GIS metadata for weather stations.

    Returns:
        pd.DataFrame: A DataFrame with PSPS probabilities and GIS metadata.
    """
    # Step 1: Count wind speed records for each weather station
    wind_speed_count = merged_data.groupby('weatherstationcode')[['wind_speed']].count()
    wind_speed_count = wind_speed_count.rename(columns={'wind_speed': 'wind_speed_count'})

    # Step 2: Count records exceeding the alert threshold
    wind_speed_above_threshold = merged_data[merged_data['wind_speed'] > merged_data[condition]]
    above_threshold_count = wind_speed_above_threshold.groupby('weatherstationcode')[['wind_speed']].count()
    above_threshold_count = above_threshold_count.rename(columns={'wind_speed': 'above_threshold_count'})

    # Step 3: Combine counts and calculate PSPS probability
    combined_count = wind_speed_count.join(above_threshold_count, how='left').fillna(0)
    combined_count['PSPS_probability'] = combined_count['above_threshold_count'] / combined_count['wind_speed_count']

    # Reset the index to include weather station code as a column
    combined_count = combined_count.reset_index()

    # Step 5: Merge with GIS weather station data
    weather_station_psps = gis_weather_station.merge(
        combined_count, on='weatherstationcode', how='left'
    )

    # Fill NaN values for PSPS-related columns
    weather_station_psps['above_threshold_count'] = weather_station_psps['above_threshold_count'].fillna(0)
    weather_station_psps['wind_speed_count'] = weather_station_psps['wind_speed_count'].fillna(0)
    weather_station_psps['PSPS_probability'] = weather_station_psps['PSPS_probability'].fillna(0)

    return weather_station_psps

def calculate_combined_count(merged_station_wind_speed):
    """
    Calculate the combined count with PSPS probability for each weather station.
    
    Args:
        merged_station_wind_speed (DataFrame): The merged station wind speed DataFrame.

    Returns:
        DataFrame: Combined count with PSPS probabilities.
    """
    # Calculate total wind speed counts
    wind_speed_count = merged_station_wind_speed.groupby('weatherstationcode')[['wind_speed']].count().rename(
        columns={'wind_speed': 'wind_speed_count'}
    )

    # Filter for wind speeds above the threshold
    windspeed_above_threshold = merged_station_wind_speed[
        merged_station_wind_speed['wind_speed'] > merged_station_wind_speed['alert']
    ]
    windspeed_count_above_threshold = windspeed_above_threshold.groupby('weatherstationcode')[['wind_speed']].count().rename(
        columns={'wind_speed': 'above_threshold_count'}
    )

    # Merge and calculate PSPS probability
    combined_count = windspeed_count_above_threshold.merge(wind_speed_count, on="weatherstationcode", how='right')
    combined_count['above_threshold_count'] = combined_count['above_threshold_count'].fillna(0)
    combined_count['PSPS_probability'] = combined_count['above_threshold_count'] / combined_count['wind_speed_count']
    
    return combined_count