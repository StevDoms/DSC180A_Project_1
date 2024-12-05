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


def main(targets):
    with open('data-params.json', 'r') as fh:
        config = json.load(fh)
    print('-------Running run.py project-------')

    # Run all pipelines if "all" target is specified
    if "all" in targets:
        targets = ["merge", "psps", "filter", "merge_vri", "analyze_spans", "feeder_analysis"]

    #Merge raw datasets of gis, station summary and wind speeds
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
        
    #Calculate PSPS probabilities for weather stations
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

    #Filter weather stations with the highest PSPS risk given a min threshold
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

    #Merge VRI (Vegetation Resource Inventory) and conductor data
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

    #Analyze spans and calculate probabilities
    if "analyze_spans" in targets and merged_data is not None:
        # Process conductor data into a GeoDataFrame
        dev_wings_agg_span = process_conductor_data(config['data_sources']['dev_wings_agg_span'])

        # Form a directed graph based on conductor spans
        G, merged_station_psps_spans = formSpanNet(merged_data, dev_wings_agg_span)

        # Identify unique upstream weather stations associated with each span
        uniqueUpsteamWStoSpan = unique_upstream_weather_stations_to_span(dev_wings_agg_span, G, merged_station_psps_spans)
        sorted_unique_upstream_weather_station_to_span = dict(sorted(uniqueUpsteamWStoSpan.items(), key=lambda item: len(item[1]), reverse=True))

        # Find the span with the highest number of upstream weather stations
        first_key, first_value = next(iter(sorted_unique_upstream_weather_station_to_span.items()))
        highest_weather_station_count = len(first_value)
        greatest_weather_station_impact = [span for span, station in sorted_unique_upstream_weather_station_to_span.items() 
                                        if len(station) == highest_weather_station_count]
        
        print("Highest Num of Stations Per Span: ", highest_weather_station_count)
        print("Example Span: ", greatest_weather_station_impact[0])
        print("Num of Spans with max Stations: ", len(greatest_weather_station_impact))

        # Calculate probabilities for each span
        new_span_probabilities = dict()

        for globalid in uniqueUpsteamWStoSpan:
            new_span_probabilities[globalid] = calculate_span_PSPS_probability(uniqueUpsteamWStoSpan[globalid], merged_data)
            
        
        # Sort spans by probability and create a DataFrame
        span_with_new_prob = dict(sorted(new_span_probabilities.items(), key=lambda item: item[1], reverse=True))
        span_with_new_prob_df = pd.DataFrame(list(span_with_new_prob.items()), columns=['span', 'probability'])
        print(span_with_new_prob_df)
        
        #Parent feeder_id 222 exploration
        #Segment data retrieval
        dev_wings_agg_span_with_probabilities = dev_wings_agg_span.merge(span_with_new_prob_df, left_on='globalid', right_on='span')
        
        segment_data = dev_wings_agg_span_with_probabilities.groupby('parent_feederid').agg(
            span_count=('globalid', 'count'),
            segment_psps_value=('probability', 'mean'),
            sum_of_customers=('cust_total', 'sum')
        )
        feeder_id = config['parameters']['parent_feeder_id']
        circuit_idx = config['parameters']['circuit_data_idx']
        
        print(segment_data[segment_data.index == feeder_id])

        #Circuit data retrieval
        circuit_data = dev_wings_agg_span_with_probabilities.groupby('upstreamardfacilityid').agg(
            span_count=('globalid', 'count'),
            circuit_psps_value=('probability', 'mean'),
            sum_of_customers=('cust_total', 'sum')
        )
        
        print(circuit_data[circuit_data.index == circuit_idx])
        
        #Get upstream weather station to span data
        upstream_weather_station_to_span = upstream_weather_stations_to_span(dev_wings_agg_span, G, merged_station_psps_spans)
        
        windspeed_snapshot_copy = load_data(windspeed_path)
        windspeed_snapshot_copy["date"] = pd.to_datetime(windspeed_snapshot_copy["date"])
        windspeed_snapshot_copy["year"] = windspeed_snapshot_copy["date"].dt.year
        
        station_stats = windspeed_snapshot_copy.groupby("station")["year"].agg(
            duration=lambda x: x.max() - x.min(),
            count="count"
        )
        
        station_stats["expected_fire_per_year"] = station_stats.apply(
            lambda row: row["count"] / row["duration"] if row["duration"] > 0 else 0, axis=1
        )
        
        expected_fire_per_year_per_station = station_stats["expected_fire_per_year"].to_dict()
        
        expected_fire_per_year_per_span = dict()
        
        for globalid, stations_per_span in upstream_weather_station_to_span.items():
            
            values = [expected_fire_per_year_per_station[tup[0]] for tup in stations_per_span]
        
            if len(values) > 0:
                expected_fire_per_year_average = np.mean(values)
            else:
                expected_fire_per_year_average = 0
        
            expected_fire_per_year_per_span[globalid] = expected_fire_per_year_average
        
        expected_fire_per_year_per_span_df = pd.DataFrame(list(expected_fire_per_year_per_span.items()), columns=['span', 'expected_fire'])
        dev_wings_agg_span_with_probabilities_expected_fire = dev_wings_agg_span_with_probabilities.merge(expected_fire_per_year_per_span_df, left_on='globalid',
                                                                                                          right_on='span')
        
        print("Span analysis completed.")

    #Perform feeder analysis
    if "feeder_analysis" in targets and dev_wings_agg_span_with_probabilities_expected_fire is not None:
        print("Parent feeder_id exploration: ", feeder_id)
        
        # Calculate annual customer count for each span
        dev_wings_agg_span_with_probabilities_expected_fire['annual_cust_total'] = dev_wings_agg_span_with_probabilities_expected_fire.apply(
            calculate_annual_customer_count, axis=1
        )
        # Summarize annual customer impacts by feeder ID
        segment_annual_customer = dev_wings_agg_span_with_probabilities_expected_fire.groupby('parent_feederid')[['annual_cust_total']].sum()
        
        # Filter data for the specified feeder ID
        feederid = segment_annual_customer[segment_annual_customer.index == feeder_id]
        print(feederid)
        
        # Calculate annual and future customer impacts based on the specified years
        annual_customer_affected = feederid.iloc[0]['annual_cust_total']
        years = config['parameters']['impact_years']
        next_years = annual_customer_affected * years
        
        print(f"Annual customers affected: {annual_customer_affected}")
        print(f"Expected Customers Affected in the next {years} years: {np.floor(next_years)}")
        print("Feeder analysis completed.")



if __name__ == "__main__":
    import sys
    targets = sys.argv[1:]
    main(targets)
