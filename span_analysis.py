
from etl import load_data, save_data
import networkx as nx
from psps import calculate_combined_count
import pandas as pd
import numpy as np

def formSpanNet(merged_station_wind_speed, dev_wings_agg_span):
    """
    Create a span network as a directed graph.

    Args:
        merged_station_wind_speed (DataFrame): Merged station wind speed data.
        dev_wings_agg_span (DataFrame): Aggregated span data.

    Returns:
        tuple: Directed graph (nx.DiGraph) and merged station spans DataFrame.
    """
    # Calculate combined count
    combined_count = calculate_combined_count(merged_station_wind_speed)

    # Merge spans with combined count
    merged_station_psps_spans = dev_wings_agg_span.merge(combined_count, left_on='station', right_index=True, how='inner')

    # Create the directed graph
    G = nx.DiGraph()
    for row in dev_wings_agg_span.itertuples(index=True, name='Pandas'):
        G.add_edge(row.globalid, row.upstream_span_id)

    return G, merged_station_psps_spans


def getUpstream(G, start_node, algorithm='dfs'):
    """
    Get all upstream nodes from the start node using the specified algorithm.
    
    Args:
        G (nx.DiGraph): The directed graph.
        start_node: The starting node.
        algorithm (str): The traversal algorithm ('bfs' or 'dfs').

    Returns:
        List: List of upstream nodes.
    """
    if algorithm.lower() == 'bfs':
        tracing = dict(nx.bfs_successors(G, start_node))
    elif algorithm.lower() == 'dfs':
        tracing = nx.dfs_successors(G, start_node)
    else:
        raise ValueError('Invalid Algorithm: Use "bfs" or "dfs".')

    return [node[0] for node in tracing.values() if node]


def getDownstream(G, start_node, algorithm='dfs'):
    """
    Retrieve downstream nodes from a given start node.

    Args:
        G (nx.DiGraph): Directed graph.
        start_node: Starting node in the graph.
        algorithm (str): Algorithm for traversal ('bfs' or 'dfs').

    Returns:
        List: Downstream nodes.
    """
    if algorithm.lower() == 'bfs':
        tracing = dict(nx.bfs_predecessors(G, start_node))
    elif algorithm.lower() == 'dfs':
        tracing = nx.dfs_predecessors(G, start_node)
    else:
        raise ValueError('Invalid Algorithm: Use "bfs" or "dfs".')

    return list(tracing.keys())



def unique_upstream_weather_stations_to_span(dev_wings_agg_span, G, merged_station_psps_spans):
    """
    Map unique upstream weather stations to each span.

    Args:
        dev_wings_agg_span (DataFrame): Aggregated span data.
        G (nx.DiGraph): Directed graph of spans.
        merged_station_psps_spans (DataFrame): Merged station and PSPS spans data.

    Returns:
        dict: Mapping of spans to unique upstream weather stations.
    """
    upstream_weather_station_to_span = dict()
    unique_upstream_weather_stations_to_span = dict()

    span_weather_station = dict()
    for row in merged_station_psps_spans.itertuples(index=True, name='Pandas'):
        span_weather_station[row.globalid] = (row.station, row.PSPS_probability, row.above_threshold_count, row.wind_speed_count)

    for row in dev_wings_agg_span.itertuples(index=True, name='Pandas'):
        upstream_spans = getUpstream(G, row.globalid, 'dfs') + [row.globalid]

        weather_stations = [span_weather_station[span] for span in upstream_spans if span in span_weather_station]
        upstream_weather_station_to_span[row.globalid] = weather_stations
        unique_upstream_weather_stations_to_span[row.globalid] = set(weather_stations)

    return unique_upstream_weather_stations_to_span
    
def upstream_weather_stations_to_span(dev_wings_agg_span, G, merged_station_psps_spans):
    """
    Map upstream weather stations to each span.

    Args:
        dev_wings_agg_span (DataFrame): Aggregated span data.
        G (nx.DiGraph): Directed graph of spans.
        merged_station_psps_spans (DataFrame): Merged station and PSPS spans data.

    Returns:
        dict: Mapping of spans to upstream weather stations.
    """    
    upstream_weather_station_to_span = dict()
    span_weather_station = dict()
    
    for row in merged_station_psps_spans.itertuples(index=True, name='Pandas'):
        span_weather_station[row.globalid] = (row.station, row.PSPS_probability, row.above_threshold_count, row.wind_speed_count)
    
    for row in dev_wings_agg_span.itertuples(index=True, name='Pandas'):
        upstream_spans = getUpstream(G, row.globalid, 'dfs') + [row.globalid]
    
        weather_stations = [span_weather_station[span] for span in upstream_spans if span in span_weather_station]
        upstream_weather_station_to_span[row.globalid] = weather_stations
    return upstream_weather_station_to_span



def calculate_span_PSPS_probability(associated_stations, merged_data):
    """
    Calculate the PSPS probability for a span.

    Args:
        associated_stations (list): List of stations associated with the span.
        merged_data (DataFrame): Merged weather station data.

    Returns:
        float: PSPS probability for the span.
    """
    overlap_wind_speeds = pd.DataFrame()
    for index, station in enumerate(associated_stations):
        filtered_df = merged_data[merged_data['weatherstationcode'] == station[0]][['date', 'exceed_threshold']].rename(
            columns={'date': f"date_{index}", 'exceed_threshold':f"exceed_threshold_{index}"}
        )
        if index == 0:
            overlap_wind_speeds = filtered_df
            continue
        
        overlap_wind_speeds = overlap_wind_speeds.merge(filtered_df, left_on=f"date_{index-1}", right_on=f"date_{index}")

    if overlap_wind_speeds.shape[0] == 0:
        return 0
        
    exceed_threshold_columns = [col for col in overlap_wind_speeds.columns if col.startswith('exceed_threshold')]
    overlap_wind_speeds['all_exceed_threshold'] = overlap_wind_speeds[exceed_threshold_columns].any(axis=1).astype(int)

    above_threshold_count = np.sum(overlap_wind_speeds['all_exceed_threshold'])
    wind_speed_count = overlap_wind_speeds.shape[0]
    
    return above_threshold_count/wind_speed_count


def calculate_annual_customer_count(row):
    annual_probability = 1 - (1 - row['probability']) ** row['expected_fire']
    annual_customers_affected = annual_probability * row['cust_total']
    return annual_customers_affected