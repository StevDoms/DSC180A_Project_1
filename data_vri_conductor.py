from etl import load_data, merge_weather_data, save_data
import geopandas as gpd
from shapely import wkt

def process_vri_data(vri_path):
    """
    Load and process the VRI dataset.
    
    Args:
        vri_path (str): Path to the VRI dataset.
    
    Returns:
        GeoDataFrame: Processed VRI GeoDataFrame.
    """
    src_vri_snapshot = load_data(vri_path)
    src_vri_snapshot['geometry'] = src_vri_snapshot['shape'].apply(wkt.loads)
    src_vri_snapshot_gpd = gpd.GeoDataFrame(src_vri_snapshot, geometry='geometry', crs=f"EPSG:{src_vri_snapshot['shape_srid'][0]}")
    
    #print(f"Processed VRI CRS: {src_vri_snapshot_gpd.crs}")
    return src_vri_snapshot_gpd


def process_conductor_data(conductor_path):
    """
    Load and process the conductor dataset.
    
    Args:
        conductor_path (str): Path to the conductor dataset.
    
    Returns:
        GeoDataFrame: Processed conductor GeoDataFrame.
    """
    dev_wings_agg_span = load_data(conductor_path)
    dev_wings_agg_span = dev_wings_agg_span.drop(columns=['Unnamed: 0'], errors='ignore')
    dev_wings_agg_span['geometry'] = dev_wings_agg_span['shape'].apply(wkt.loads)
    dev_wings_agg_span_gpd = gpd.GeoDataFrame(dev_wings_agg_span, geometry='geometry', crs=f"EPSG:{dev_wings_agg_span['shape_srid'][0]}")
    
    return dev_wings_agg_span_gpd



def merge_psps_conductor_vri(conductor_path, vri_path, weather_station_psps):
    dev_wings_agg_span = process_conductor_data(conductor_path)
    #dev_wings_agg_span = dev_wings_agg_span.drop(columns=['Unnamed: 0'])
    src_vri_snapshot = process_vri_data(vri_path)

    weather_station_psps['geometry'] = weather_station_psps['shape'].apply(wkt.loads)
    weather_station_psps_gpd = gpd.GeoDataFrame(weather_station_psps, geometry='geometry', crs=f"EPSG:{weather_station_psps['shape_srid'][0]}")

    src_vri_snapshot['geometry'] = src_vri_snapshot['shape'].apply(wkt.loads)
    src_vri_snapshot_gpd = gpd.GeoDataFrame(src_vri_snapshot, geometry='geometry', crs=f"EPSG:{src_vri_snapshot['shape_srid'][0]}")

    dev_wings_agg_span['geometry'] = dev_wings_agg_span['shape'].apply(wkt.loads)
    dev_wings_agg_span_gpd = gpd.GeoDataFrame(dev_wings_agg_span, geometry='geometry', crs=f"EPSG:{dev_wings_agg_span['shape_srid'][0]}")
    
    # print(f"Weather Station CRS:    {weather_station_psps_gpd.crs}")
    # print(f"VRI Polygon CRS:        {src_vri_snapshot_gpd.crs}")
    # print(f"Conductor Span CRS:     {dev_wings_agg_span_gpd.crs}")

    weather_station_psps_gpd = weather_station_psps_gpd.to_crs(src_vri_snapshot_gpd.crs)
    dev_wings_agg_span_gpd = dev_wings_agg_span_gpd.to_crs(src_vri_snapshot_gpd.crs)

    # print(f"Weather Station CRS:    {weather_station_psps_gpd.crs}")
    # print(f"VRI Polygon CRS:        {src_vri_snapshot_gpd.crs}")
    # print(f"Conductor Span CRS:     {dev_wings_agg_span_gpd.crs}")

    merged_station_vri_gpd = gpd.sjoin(weather_station_psps_gpd, src_vri_snapshot_gpd, predicate="within", how="inner")
    merged_station_vri_gpd = merged_station_vri_gpd.sort_index()

    # print(merged_station_vri_gpd.head())
    
    merged_station_vri_spans_gpd = gpd.sjoin(src_vri_snapshot_gpd, dev_wings_agg_span_gpd, how="inner", predicate="intersects")
    merged_station_vri_spans_gpd = merged_station_vri_spans_gpd

    # print(merged_station_vri_spans_gpd.head())
    return merged_station_vri_spans_gpd
