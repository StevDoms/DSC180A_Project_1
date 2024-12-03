import numpy as np
import pandas as pd

def get_feeder_spans(feeder_id, merged_station_psps_spans):
    """
    Get spans associated with a specific feeder ID.
    
    Args:
        feeder_id (str): The feeder ID to analyze.
        merged_station_psps_spans (DataFrame): DataFrame with span and feeder information.
    
    Returns:
        List[str]: List of spans associated with the feeder ID.
    """
    return [
        row.globalid for row in merged_station_psps_spans.itertuples(index=True, name='Pandas')
        if row.parent_feederid == feeder_id
    ]


def calculate_span_shutoff_probability(parent_feeder_spans, new_span_probabilities):
    """
    Calculate the probability of any span within the feeder being shut off.
    
    Args:
        parent_feeder_spans (List[str]): List of spans associated with the feeder ID.
        new_span_probabilities (dict): A dictionary of span probabilities.
    
    Returns:
        float: Probability of any span being shut off.
    """
    parent_feeder_probabilities = [
        new_span_probabilities[span] for span in parent_feeder_spans if span in new_span_probabilities
    ]
    return np.mean(parent_feeder_probabilities) if parent_feeder_probabilities else 0


def calculate_total_customers(feeder_id, merged_station_psps_spans):
    """
    Calculate the total number of customers for a given feeder ID.
    
    Args:
        feeder_id (str): The feeder ID to analyze.
        merged_station_psps_spans (DataFrame): DataFrame with span and feeder information.
    
    Returns:
        int: Total number of customers.
    """
    return merged_station_psps_spans.loc[
        merged_station_psps_spans['parent_feederid'] == feeder_id, 'cust_total'
    ].sum()

def calculate_customer_impact(total_customers, prob_of_any_span_shutoff, years=10):
    """
    Calculate the annual and multi-year customer impact.
    
    Args:
        total_customers (int): Total number of customers.
        prob_of_any_span_shutoff (float): Probability of any span being shut off.
        years (int): Number of years to calculate the impact for (default is 10).
    
    Returns:
        dict: Annual and multi-year customer impact.
    """
    annual_customers_affected = total_customers * prob_of_any_span_shutoff
    multi_year_customers_affected = annual_customers_affected * years

    return {
        "Annual Customers Affected": annual_customers_affected,
        f"Next {years} Years Customers Affected": np.floor(multi_year_customers_affected),
    }


def calculateFeederImpact(feeder_id, dev_wings_agg_span, combined_count, new_span_probabilities, years=10):
    """
    Main function to calculate impact statistics for a given feeder ID.
    
    Args:
        feeder_id (str): The feeder ID to analyze.
        dev_wings_agg_span (DataFrame): DataFrame containing span and feeder information.
        combined_count (DataFrame): DataFrame containing combined counts and probabilities.
        new_span_probabilities (dict): A dictionary where keys are spans and values are probabilities.
        years (int): Number of years to calculate the impact for (default is 10).
    
    Returns:
        dict: A dictionary with calculated statistics for the feeder ID.
    """
    # Merge spans with combined count
    merged_station_psps_spans = dev_wings_agg_span.merge(combined_count, left_on='station', right_index=True, how='inner')

    # Calculate intermediate results
    parent_feeder_spans = get_feeder_spans(feeder_id, merged_station_psps_spans)
    num_spans = len(parent_feeder_spans)
    prob_of_any_span_shutoff = calculate_span_shutoff_probability(parent_feeder_spans, new_span_probabilities)
    total_customers = calculate_total_customers(feeder_id, merged_station_psps_spans)
    customer_impact = calculate_customer_impact(total_customers, prob_of_any_span_shutoff, years)

    # Aggregate results
    results = {
        "Feeder ID": feeder_id,
        "Number of Spans": num_spans,
        "Probability of Shut-off": prob_of_any_span_shutoff,
        "Total Customers": total_customers,
        **customer_impact
    }

    return results
