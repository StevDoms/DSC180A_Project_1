import pandas as pd
import os
from psps import calculate_psps_probability
import geopandas as gpd
from shapely import wkt

def load_data(file_path):
    """Loads data from a CSV file into a pandas DataFrame."""
    return pd.read_csv(file_path, low_memory = False)

def get_gis_data(gis_path):
    """
    Loads and returns the GIS data.
    
    Args:
        gis_path (str): Path to the GIS data file.
    
    Returns:
        DataFrame: Loaded GIS data.
    """
    gis_data = load_data(gis_path)
    print(f"GIS Data loaded with {gis_data.shape[0]} rows and {gis_data.shape[1]} columns.")
    return gis_data

def get_station_summary_data(station_summary_path):
    """
    Loads and returns the station summary data.
    
    Args:
        station_summary_path (str): Path to the station summary data file.
    
    Returns:
        DataFrame: Loaded station summary data.
    """
    station_summary = load_data(station_summary_path)
    print(f"Station Summary Data loaded with {station_summary.shape[0]} rows and {station_summary.shape[1]} columns.")
    return station_summary

def get_windspeed_data(windspeed_path):
    """
    Loads and returns the windspeed data.
    
    Args:
        windspeed_path (str): Path to the windspeed data file.
    
    Returns:
        DataFrame: Loaded windspeed data.
    """
    windspeed_data = load_data(windspeed_path)
    print(f"Windspeed Data loaded with {windspeed_data.shape[0]} rows and {windspeed_data.shape[1]} columns.")
    return windspeed_data

def merge_weather_data(gis_path, station_summary_path, windspeed_path):
    """Merges the GIS, station summary, and windspeed datasets."""
    # Load datasets
    gis_data = get_gis_data(gis_path)
    station_summary = get_station_summary_data(station_summary_path)
    windspeed_snapshot = get_windspeed_data(windspeed_path)

    # Merging logic
    merged_data = gis_data.merge(station_summary, left_on='weatherstationcode', right_on='station').drop(columns=['station'])
    final_merged_data = merged_data.merge(windspeed_snapshot, left_on='weatherstationcode', right_on='station').drop(columns=['station'])
    return final_merged_data

def save_data(df, output_path, file_name):
    """Saves the processed DataFrame to a CSV file."""
    os.makedirs(output_path, exist_ok=True)
    file_path = os.path.join(output_path, file_name)
    df.to_csv(file_path, index=False)
    return file_path
