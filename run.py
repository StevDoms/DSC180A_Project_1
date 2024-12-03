import json
from run_functions import (
    run_data_processing,
    run_psps_calculation,
    run_top_psps_filter,
    run_vri_conductor_merge,
    run_network_build,
    run_associate_station_span,
    run_span_probabilities,
    run_highest_prob_spans,
    run_feeder_info,
)

def main(targets):
    with open('data-params.json', 'r') as fh:
        config = json.load(fh)

    if 'data' in targets:
        print("\n-------- Starting Data Processing --------")
        run_data_processing(config)
        print("-------- Data Processing Completed --------\n")

    if 'psps' in targets:
        print("\n-------- Calculating PSPS Probabilities --------")
        run_psps_calculation(config)
        print("-------- PSPS Probabilities Calculation Completed --------\n")

    if 'top-psps' in targets:
        print("\n-------- Filtering Top PSPS Stations --------")
        run_top_psps_filter(config)
        print("-------- Top PSPS Filtering Completed --------\n")

    if 'vri-conductor' in targets:
        print("\n-------- Merging VRI and Conductor Data --------")
        run_vri_conductor_merge(config)
        print("-------- VRI-Conductor Merge Completed --------\n")

    if 'network' in targets:
        print("\n-------- Building Span Network --------")
        run_network_build(config)
        print("-------- Span Network Build Completed --------\n")

    if 'associateStationSpan' in targets:
        print("\n-------- Associating Weather Stations with Spans --------")
        run_associate_station_span(config)
        print("-------- Association Completed --------\n")

    if 'getSpanProbs' in targets:
        print("\n-------- Calculating Span Probabilities --------")
        run_span_probabilities(config)
        print("-------- Span Probabilities Calculation Completed --------\n")

    # if 'highestProbSpans' in targets:
    #     print("\n-------- Fetching Spans with Highest Probabilities --------")
    #     run_highest_prob_spans(config)
    #     print("-------- Spans with Highest Probabilities Process Completed --------\n")

    # if 'feederInfo' in targets:
    #     print("\n-------- Calculating Feeder Impact --------")
    #     run_feeder_info(config)
    #     print("-------- Feeder Impact Calculation Completed --------\n")

    if 'all' in targets:
        print("\n-------- Running Full Pipeline --------")
        main(['data', 'psps', 'top-psps', 'vri-conductor', 'network', 'associateStationSpan', 'highestProbSpans', 'getSpanProbs', 'feederInfo'])
        print("-------- Full Pipeline Execution Completed --------\n")

if __name__ == '__main__':
    import sys
    targets = sys.argv[1:]
    if not targets:
        print("Please specify a target. Options:")
        print("data, psps, top-psps, vri-conductor, network, associateStationSpan, getSpanProbs, highestProbSpans, feederInfo, all")
    else:
        main(targets)
