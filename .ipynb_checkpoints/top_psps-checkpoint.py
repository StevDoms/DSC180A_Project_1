import pandas as pd

def filter_top_psps_stations(psps_data, threshold):
    """Filters weather stations with PSPS probability above the given threshold.

    Args:
        psps_data (pd.DataFrame): DataFrame containing PSPS probabilities for weather stations.
        threshold (float): The probability threshold for filtering.

    Returns:
        pd.DataFrame: A filtered DataFrame with weather stations exceeding the threshold.
    """
    return psps_data[psps_data['PSPS_probability'] > threshold]
