
import os
import json
import pandas as pd
from etl import load_data, save_data, merge_weather_data
from psps import calculate_psps_probability, calculate_combined_count
from top_psps import filter_top_psps_stations
from data_vri_conductor import merge_psps_conductor_vri, process_conductor_data
from span_analysis import (
    formSpanNet,
    getAssociatedStationToSpan,
    getSpanWithHighestStationImpact,
    getSpanProbabilities,
    getSpansWithHighestProb,
    unique_upstream_weather_stations_to_span,
    calculate_span_PSPS_probability,
)
from feederInfo import calculateFeederImpact


def check_dependency(file_path, dependency_name):
    """
    Check if a required file exists, and raise an error if it doesn't.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"Dependency check failed: '{file_path}' not found. Please run the '{dependency_name}' step first."
        )


def run_data_processing(config):
    print("-------- Running Data Processing --------")
    
    # Paths to raw data sources
    gis_path = config['data_sources']['gis_weatherstation']
    station_summary_path = config['data_sources']['station_summary_snapshot']
    windspeed_path = config['data_sources']['windspeed_snapshot']

    # Merge raw datasets
    merged_data = merge_weather_data(gis_path, station_summary_path, windspeed_path)
    
    # Save the processed dataset
    save_data(merged_data, config['output_paths']['processed_data'], 'merged_weather_data.csv')
    
    print("Data processing completed. Merged weather data saved.")
    print("Preview of merged weather data:")
    print(merged_data.head())


def run_psps_calculation(config):
    merged_weather_data_path = config['output_paths']['processed_data'] + 'merged_weather_data.csv'
    check_dependency(merged_weather_data_path, 'data')

    gis_weather_station = load_data(config['data_sources']['gis_weatherstation'])
    merged_data = load_data(merged_weather_data_path)
    weather_station_psps = calculate_psps_probability(
        merged_data, gis_weather_station, config['parameters']['psps_condition']
    )
    save_data(weather_station_psps, config['output_paths']['processed_data'], 'weather_station_psps.csv')
    print(f"PSPS probabilities calculated for {len(weather_station_psps)} stations.")
    print("Preview of PSPS probabilities:")
    print(weather_station_psps.head())


def run_top_psps_filter(config):
    psps_data_path = config['output_paths']['processed_data'] + 'weather_station_psps.csv'
    check_dependency(psps_data_path, 'psps')

    weather_station_psps = load_data(psps_data_path)
    top_stations = filter_top_psps_stations(weather_station_psps, config['parameters']['min_alert_threshold'])
    save_data(top_stations, config['output_paths']['processed_data'], 'top_psps_stations.csv')
    print(f"Filtered {len(top_stations)} high-risk PSPS stations.")
    print("Preview of top PSPS stations:")
    print(top_stations.head())


def run_vri_conductor_merge(config):
    psps_data_path = config['output_paths']['processed_data'] + 'weather_station_psps.csv'
    check_dependency(psps_data_path, 'psps')

    vri_path = config['data_sources']['src_vri_snapshot']
    conductor_path = config['data_sources']['dev_wings_agg_span']
    weather_station_psps = load_data(psps_data_path)
    conductor_vri_psps = merge_psps_conductor_vri(conductor_path, vri_path, weather_station_psps)
    save_data(conductor_vri_psps, config['output_paths']['processed_data'], 'merged_vri_conductor_psps.csv')
    print(f"Merged VRI and conductor data saved. Total rows: {len(conductor_vri_psps)}.")
    print("Preview of merged VRI-conductor data:")
    print(conductor_vri_psps.head())


def run_network_build(config):
    merged_weather_data_path = config['output_paths']['processed_data'] + 'merged_weather_data.csv'
    check_dependency(merged_weather_data_path, 'data')

    merged_station_wind_speed = load_data(merged_weather_data_path)
    dev_wings_agg_span = process_conductor_data(config['data_sources']['dev_wings_agg_span'])
    G, _ = formSpanNet(merged_station_wind_speed, dev_wings_agg_span)
    network_data = {"nodes": list(G.nodes), "edges": list(G.edges)}
    output_path = config['output_paths']['processed_data']
    with open(f"{output_path}/network_graph.json", 'w') as json_file:
        json.dump(network_data, json_file)
    print(f"Network graph saved with {len(G.nodes)} nodes and {len(G.edges)} edges.")


def run_associate_station_span(config):

    merged_weather_data_path = config['output_paths']['processed_data'] + 'merged_weather_data.csv'
    check_dependency(merged_weather_data_path, 'data')

    dev_wings_agg_span = load_data(config['data_sources']['dev_wings_agg_span'])
    merged_station_wind_speed = load_data(merged_weather_data_path)

    G, merged_station_psps_spans = formSpanNet(merged_station_wind_speed, dev_wings_agg_span)

    uniqueUpsteamWStoSpan_ = unique_upstream_weather_stations_to_span(dev_wings_agg_span, G, merged_station_psps_spans)
    sorted_unique_upstream_weather_station_to_span = dict(sorted(uniqueUpsteamWStoSpan_.items(), key=lambda item: len(item[1]), reverse=True))

    first_key, first_value = next(iter(sorted_unique_upstream_weather_station_to_span.items()))
    highest_weather_station_count = len(first_value)
    greatest_weather_station_impact = [span for span, station in sorted_unique_upstream_weather_station_to_span.items() 
                                    if len(station) == highest_weather_station_count]

    print("Highest Num of Stations Per Span: ", highest_weather_station_count)
    print("Example Span: ", greatest_weather_station_impact[0])
    print("Num of Spans with max Stations: ", len(greatest_weather_station_impact))


def run_span_probabilities(config):
    print('ran')
    merged_weather_data_path = config['output_paths']['processed_data'] + 'merged_weather_data.csv'
    check_dependency(merged_weather_data_path, 'data')

    dev_wings_agg_span = load_data(config['data_sources']['dev_wings_agg_span'])
    merged_station_wind_speed = load_data(merged_weather_data_path)
    G, merged_station_psps_spans = formSpanNet(merged_station_wind_speed, dev_wings_agg_span)

    new_span_probabilities = dict()
    uniqueUpsteamWStoSpan = unique_upstream_weather_stations_to_span(dev_wings_agg_span, G, merged_station_psps_spans)
    
    count = len(uniqueUpsteamWStoSpan)
    print(f"Number of spans in uniqueUpsteamWStoSpan: {count}")

    for globalid in uniqueUpsteamWStoSpan:
        new_span_probabilities[globalid] = calculate_span_PSPS_probability(uniqueUpsteamWStoSpan[globalid], merged_station_wind_speed)
        print(globalid, new_span_probabilities[globalid])

    span_with_new_prob = dict(sorted(new_span_probabilities.items(), key=lambda item: item[1], reverse=True))
    span_with_new_prob_df = pd.DataFrame(list(span_with_new_prob.items()), columns=['span', 'probability'])

    span_probabilities_df = pd.DataFrame(new_span_probabilities.items(), columns=["Span", "Probability"])
    save_data(span_probabilities_df, config['output_paths']['processed_data'], 'span_probabilities.csv')
    print(f"Calculated probabilities for {len(new_span_probabilities)} spans.")
    print("Span probabilities saved to 'span_probabilities.csv'.")
    print("Preview of span probabilities:")
    print(span_probabilities_df.head())


def run_highest_prob_spans(config):
    span_probabilities_path = config['output_paths']['processed_data'] + 'span_probabilities.csv'
    check_dependency(span_probabilities_path, 'getSpanProbs')

    span_probabilities_df = pd.read_csv(span_probabilities_path)
    new_span_probabilities = dict(zip(span_probabilities_df["Span"], span_probabilities_df["Probability"]))
    span_with_highest_prob_df = getSpansWithHighestProb(new_span_probabilities)
    save_data(span_with_highest_prob_df, config['output_paths']['processed_data'], 'spans_with_highest_probabilities.csv')
    print(f"Spans with highest probabilities saved. Total spans: {len(span_with_highest_prob_df)}.")
    print("Preview of spans with highest probabilities:")
    print(span_with_highest_prob_df.head())


def run_feeder_info(config):
    merged_weather_data_path = config['output_paths']['processed_data'] + 'merged_weather_data.csv'
    span_probabilities_path = config['output_paths']['processed_data'] + 'span_probabilities.csv'

    # Check dependencies
    check_dependency(merged_weather_data_path, 'data')
    check_dependency(span_probabilities_path, 'getSpanProbs')

    dev_wings_agg_span = load_data(config['data_sources']['dev_wings_agg_span'])
    merged_station_wind_speed = load_data(merged_weather_data_path)
    combined_count = calculate_combined_count(merged_station_wind_speed)

    G, merged_station_psps_spans = formSpanNet(merged_station_wind_speed, dev_wings_agg_span)
    associated_weather_station_to_span = getAssociatedStationToSpan(G, merged_station_psps_spans)
    new_span_probabilities = getSpanProbabilities(associated_weather_station_to_span)

    # Retrieve parameters
    feeder_id = config['parameters'].get('parent_feeder_id', '222')
    years = config['parameters'].get('impact_years', 10)

    feeder_impact = calculateFeederImpact(
        feeder_id=feeder_id,
        dev_wings_agg_span=dev_wings_agg_span,
        combined_count=combined_count,
        new_span_probabilities=new_span_probabilities,
        years=years
    )

    output_path = config['output_paths']['processed_data'] + 'feeder_impact_statistics.json'
    with open(output_path, 'w') as json_file:
        json.dump(feeder_impact, json_file, indent=4)

    # Print results
    print(f"Feeder impact statistics saved for feeder {feeder_id}.")
    print("Summary of feeder impact statistics:")
    for key, value in feeder_impact.items():
        print(f"{key}: {value}")
