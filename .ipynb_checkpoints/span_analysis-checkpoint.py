
from etl import load_data, save_data
import networkx as nx
from psps import calculate_combined_count
import pandas as pd
import numpy as np
def formSpanNet(merged_station_wind_speed, dev_wings_agg_span):
    """
    Form the span network based on the provided data.
    
    Args:
        merged_station_wind_speed (DataFrame): The merged station wind speed DataFrame.
        dev_wings_agg_span (DataFrame): The aggregated span DataFrame.

    Returns:
        nx.DiGraph: The directed graph representing the span network.
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
    Get all downstream nodes from the start node using the specified algorithm.
    
    Args:
        G (nx.DiGraph): The directed graph.
        start_node: The starting node.
        algorithm (str): The traversal algorithm ('bfs' or 'dfs').

    Returns:
        List: List of downstream nodes.
    """
    if algorithm.lower() == 'bfs':
        tracing = dict(nx.bfs_predecessors(G, start_node))
    elif algorithm.lower() == 'dfs':
        tracing = nx.dfs_predecessors(G, start_node)
    else:
        raise ValueError('Invalid Algorithm: Use "bfs" or "dfs".')

    return list(tracing.keys())


def getAssociatedStationToSpan(G, merged_station_psps_spans):
    """
    Get the associated weather stations to each span.
    
    Args:
        G (nx.DiGraph): The directed graph representing the span network.
        merged_station_psps_spans (DataFrame): The merged spans and station data.

    Returns:
        Dict: A dictionary mapping spans to associated weather stations.
    """
    span_weather_station = {
        row.globalid: (row.station, row.PSPS_probability)
        for row in merged_station_psps_spans.itertuples(index=True, name='Pandas')
    }

    associated_weather_station_to_span = {}

    for row in merged_station_psps_spans.itertuples(index=True, name='Pandas'):
        upstream_spans = getUpstream(G, row.globalid, 'dfs') + [row.globalid]
        weather_stations = {
            span_weather_station[span]
            for span in upstream_spans
            if span in span_weather_station
        }
        associated_weather_station_to_span[row.globalid] = weather_stations

    return associated_weather_station_to_span

def getSpanWithHighestStationImpact(associated_weather_station_to_span):
    """
    Finds the span(s) with the highest number of associated weather stations.
    
    Args:
        associated_weather_station_to_span (dict): Dictionary mapping spans to associated weather stations.

    Returns:
        dict: A dictionary containing:
            - 'highest_count': The highest number of weather stations for any span.
            - 'example_span': An example span with the highest number of stations.
            - 'spans_with_highest_count': List of spans with the highest number of stations.
            - 'num_spans_with_highest_count': Number of spans with the highest count.
    """
    # Sort spans by the number of associated weather stations (descending order)
    sorted_associated_weather_station_to_span = dict(
        sorted(associated_weather_station_to_span.items(), key=lambda item: len(item[1]), reverse=True)
    )

    # Get the first key-value pair (span with the highest number of stations)
    first_key, first_value = next(iter(sorted_associated_weather_station_to_span.items()))
    highest_weather_station_count = len(first_value)

    # Find all spans with the highest number of weather stations
    greatest_weather_station_impact = [
        span for span, station in sorted_associated_weather_station_to_span.items()
        if len(station) == highest_weather_station_count
    ]

    # # Print the results
    # print("Highest Num of Stations Per Span: ", highest_weather_station_count)
    # print("Example Span: ", greatest_weather_station_impact[0])
    # print("Num of Spans with max Stations: ", len(greatest_weather_station_impact))

    # # Return results as a dictionary
    return {
        "highest_count": highest_weather_station_count,
        "example_span": greatest_weather_station_impact[0],
        "spans_with_highest_count": greatest_weather_station_impact,
        "num_spans_with_highest_count": len(greatest_weather_station_impact),
    }

def calculate_span_off_from_off_probs(off_probs):
    """
    Calculate the probability of a span being off given probabilities of connected stations.
    
    Args:
        off_probs (list of tuples): A list of tuples where each tuple contains (station, PSPS_probability).

    Returns:
        float: The probability of the span being off.
    """
    n = len(off_probs)
    if n == 0:
        return 0

    on_probs = [1 - p for _, p in off_probs]  # Convert off probabilities to on probabilities

    p_all_on = 1.0
    for p_on in on_probs:
        p_all_on *= p_on

    # Probability of the span being off
    p_last_off = 1 - p_all_on
    return p_last_off


def getSpanProbabilities(associated_weather_station_to_span):
    """
    Calculate the probabilities of each span being off based on associated weather station probabilities.
    
    Args:
        associated_weather_station_to_span (dict): A dictionary mapping spans to their associated weather stations
                                                   and PSPS probabilities.

    Returns:
        dict: A dictionary mapping spans to their calculated "off" probabilities.
    """
    new_span_probabilities = {}

    for span, stations in associated_weather_station_to_span.items():
        new_span_probabilities[span] = calculate_span_off_from_off_probs(stations)
    
    return new_span_probabilities

def getSpansWithHighestProb(new_span_probabilities):
    """
    Sorts spans by their probabilities in descending order and returns a DataFrame.
    
    Args:
        new_span_probabilities (dict): A dictionary where keys are spans and values are probabilities.
    
    Returns:
        pd.DataFrame: A DataFrame containing spans sorted by probabilities.
    """
    # Sort the spans by probability in descending order
    span_with_highest_prob = dict(sorted(new_span_probabilities.items(), key=lambda item: item[1], reverse=True))
    
    # Convert the sorted dictionary to a DataFrame
    span_with_highest_prob_df = pd.DataFrame(list(span_with_highest_prob.items()), columns=['Span', 'Probability'])
    
    return span_with_highest_prob_df


def unique_upstream_weather_stations_to_span(dev_wings_agg_span, G, merged_station_psps_spans):
    print('run')
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
    
    upstream_weather_station_to_span = dict()
    span_weather_station = dict()
    
    for row in merged_station_psps_spans.itertuples(index=True, name='Pandas'):
        span_weather_station[row.globalid] = (row.station, row.PSPS_probability, row.above_threshold_count, row.wind_speed_count)
    
    for row in dev_wings_agg_span.itertuples(index=True, name='Pandas'):
        upstream_spans = getUpstream(G, row.globalid, 'dfs') + [row.globalid]
    
        weather_stations = [span_weather_station[span] for span in upstream_spans if span in span_weather_station]
        upstream_weather_station_to_span[row.globalid] = weather_stations
    return upstream_weather_station_to_span


def calculate_span_PSPS_probability(associated_stations):
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
