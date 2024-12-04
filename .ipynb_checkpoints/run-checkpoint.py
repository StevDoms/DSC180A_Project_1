import numpy as np
import os
import json
import pandas as pd
from etl import load_data, save_data, merge_weather_data
from psps import calculate_psps_probability, calculate_combined_count
from top_psps import filter_top_psps_stations
from data_vri_conductor import merge_psps_conductor_vri, process_conductor_data
from span_analysis import (
    formSpanNet,
    unique_upstream_weather_stations_to_span,
    calculate_span_PSPS_probability,
    upstream_weather_stations_to_span,
    getUpstream,
    calculate_annual_customer_count
)
from feederInfo import calculateFeederImpact


def main(targets):
    with open('data-params.json', 'r') as fh:
        config = json.load(fh)

    print('Running run.py project')

    if "all" in targets:
        targets = ["merge", "psps", "filter", "merge_vri", "analyze_spans", "feeder_analysis"]

    if "merge" in targets:
        print("Merging raw datasets...")
        gis_path = config['data_sources']['gis_weatherstation']
        station_summary_path = config['data_sources']['station_summary_snapshot']
        windspeed_path = config['data_sources']['windspeed_snapshot']
        merged_data = merge_weather_data(gis_path, station_summary_path, windspeed_path)
        print("Data processing completed. Merged weather data saved.")
        print("Preview of merged weather data:")
    else:
        merged_data = None

    if "psps" in targets and merged_data is not None:
        print("Calculating PSPS probabilities...")
        gis_weather_station = load_data(config['data_sources']['gis_weatherstation'])
        weather_station_psps = calculate_psps_probability(
            merged_data, gis_weather_station, config['parameters']['psps_condition']
        )
        print(f"PSPS probabilities calculated for {len(weather_station_psps)} stations.")
        print("Preview of PSPS probabilities:")
        print(weather_station_psps.head())
    else:
        weather_station_psps = None

    if "filter" in targets and weather_station_psps is not None:
        print("Filtering top PSPS stations...")
        top_stations = filter_top_psps_stations(
            weather_station_psps, config['parameters']['min_alert_threshold']
        )
        print(f"Filtered {len(top_stations)} high-risk PSPS stations.")
        print("Preview of top PSPS stations:")
        print(top_stations.head())
    else:
        top_stations = None

    if "merge_vri" in targets and weather_station_psps is not None:
        print("Merging VRI and conductor data...")
        vri_path = config['data_sources']['src_vri_snapshot']
        conductor_path = config['data_sources']['dev_wings_agg_span']
        conductor_vri_psps = merge_psps_conductor_vri(conductor_path, vri_path, weather_station_psps)
        print(f"Merged VRI and conductor data saved. Total rows: {len(conductor_vri_psps)}.")
        print("Preview of merged VRI-conductor data:")
        print(conductor_vri_psps.head())
    else:
        conductor_vri_psps = None

    if "analyze_spans" in targets and merged_data is not None:
        print("Forming network spans and span exploration...")
        dev_wings_agg_span = process_conductor_data(config['data_sources']['dev_wings_agg_span'])
        G, merged_station_psps_spans = formSpanNet(merged_data, dev_wings_agg_span)
        uniqueUpsteamWStoSpan = unique_upstream_weather_stations_to_span(
            dev_wings_agg_span, G, merged_station_psps_spans
        )
        sorted_unique_upstream_weather_station_to_span = dict(
            sorted(uniqueUpsteamWStoSpan.items(), key=lambda item: len(item[1]), reverse=True)
        )
        first_key, first_value = next(iter(sorted_unique_upstream_weather_station_to_span.items()))
        highest_weather_station_count = len(first_value)
        greatest_weather_station_impact = [
            span for span, station in sorted_unique_upstream_weather_station_to_span.items()
            if len(station) == highest_weather_station_count
        ]
        print("Highest Num of Stations Per Span: ", highest_weather_station_count)
        print("Example Span: ", greatest_weather_station_impact[0])
        print("Num of Spans with max Stations: ", len(greatest_weather_station_impact))

        new_span_probabilities = dict()
        
        for globalid in uniqueUpsteamWStoSpan:
            new_span_probabilities[globalid] = calculate_span_PSPS_probability(uniqueUpsteamWStoSpan[globalid], merged_data)
            
        span_with_new_prob = dict(sorted(new_span_probabilities.items(), key=lambda item: item[1], reverse=True))
        span_with_new_prob_df = pd.DataFrame(list(span_with_new_prob.items()), columns=['span', 'probability'])
        print('Span with new probabilities preview')
        print(span_with_new_prob_df.head())
    else:
        dev_wings_agg_span, span_with_new_prob_df = None, None

    if "feeder_analysis" in targets and dev_wings_agg_span is not None:
        print("Parent feeder_id exploration...")
    
        dev_wings_agg_span_with_probabilities = dev_wings_agg_span.merge(
            span_with_new_prob_df, left_on='globalid', right_on='span'
        )
    
        segment_data = dev_wings_agg_span_with_probabilities.groupby('parent_feederid').agg(
            span_count=('globalid', 'count'),
            segment_psps_value=('probability', 'mean'),
            sum_of_customers=('cust_total', 'sum')
        )
    
        target_feeder_id = config['parameters']['target_feeder_id']
        if target_feeder_id in segment_data.index:
            print(segment_data[segment_data.index == target_feeder_id])
        else:
            print(f"Feeder ID {target_feeder_id} not found in segment data.")
    
        # Circuit analysis
        circuit_data = dev_wings_agg_span_with_probabilities.groupby('upstreamardfacilityid').agg(
            span_count=('globalid', 'count'),
            circuit_psps_value=('probability', 'mean'),
            sum_of_customers=('cust_total', 'sum')
        )
    
        target_circuit_id = config['parameters']['target_circuit_id']
        if target_circuit_id in circuit_data.index:
            print(circuit_data[circuit_data.index == target_circuit_id])
        else:
            print(f"Circuit ID {target_circuit_id} not found in circuit data.")
    
        # Customer annual count
        segment_annual_customer = dev_wings_agg_span_with_probabilities.groupby('parent_feederid')[['annual_cust_total']].sum()
        if target_feeder_id in segment_annual_customer.index:
            feeder_data = segment_annual_customer[segment_annual_customer.index == target_feeder_id]
            print(feeder_data)
    
            annual_customer_affected = feeder_data.iloc[0]['annual_cust_total']
            next_10_years = annual_customer_affected * 10
            print(f"Annual customers affected: {annual_customer_affected}")
            print(f"Expected Customers Affected in the next 10 years: {np.floor(next_10_years)}")
        else:
            print(f"Feeder ID {target_feeder_id} not found in annual customer data.")

    print("Finished process!")


if __name__ == "__main__":
    import sys
    targets = sys.argv[1:]
    main(targets)
