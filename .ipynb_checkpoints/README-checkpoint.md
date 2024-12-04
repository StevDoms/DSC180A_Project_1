Here’s the updated content with instructions for running individual steps:

---

# DSC180A Quarter 1 Capstone Project: Enhancing Wildfire Resilience Through Strategic Public Safety Power Shutoffs

This Quarter 1 Capstone Project focuses on implementing Data Science methods to provide insights into Public Safety Power Shutoffs (PSPS) and developing actionable strategies for wildfire mitigation. Collaborating with SDG&E, we explore PSPS probabilities, assigning a probability value to every span on the SDG&E grid. Using exploratory data analysis (EDA), geospatial mapping, and directed graph analysis, we enable upstream and downstream tracing of spans.

## How to Run the Code

### Prerequisites

1. Ensure you have [Anaconda](https://www.anaconda.com/products/distribution) installed.
2. Ensure that the required data files are present in the `data/` folder, structured as follows:
    ```
    data/
        gis_weatherstation_shape_2024_10_04.csv
        src_wings_meteorology_station_summary_snapshot_2023_08_02.csv
        src_wings_meteorology_windspeed_snapshot_2023_08_02.csv
        src_vri_snapshot_2024_03_20.csv
        dev_wings_agg_span_2024_01_01.csv
    data/processed/  # This folder will store processed outputs
    ```

---

### Step 1: Set Up the Environment

1. Open a terminal and navigate to the project directory:
   ```bash
   cd /path/to/DSC180A_project
   ```

2. Run the following commands to set up the Conda environment:
   ```bash
   conda env create -f environment.yml
   conda activate geo_env
   ```

3. After the Conda environment is activated, you are prepared to run the project code.

---

### Step 2: Run the Whole Pipeline

To execute the entire pipeline, use the following command:
```bash
python run.py all
```

---

### Step 3: Run Individual Steps

If you need to run specific components of the pipeline, use the following commands:

- **Data Processing**: Prepares and merges weather data.
  ```bash
  python run.py data
  ```

- **PSPS Probabilities**: Calculates PSPS probabilities for weather stations.
  ```bash
  python run.py psps
  ```

- **Top PSPS Stations**: Filters high-risk PSPS stations based on a threshold.
  ```bash
  python run.py top-psps
  ```

- **VRI and Conductor Merge**: Merges conductor and vegetation risk index (VRI) data.
  ```bash
  python run.py vri-conductor
  ```

- **Network Graph**: Builds a directed graph of spans for upstream/downstream analysis.
  ```bash
  python run.py network
  ```

- **Associate Weather Stations with Spans**: Links weather stations to spans.
  ```bash
  python run.py associateStationSpan
  ```

- **Span Probabilities**: Calculates probabilities for spans being affected.
  ```bash
  python run.py getSpanProbs
  ```

- **Highest Probability Spans**: Identifies spans with the highest shutoff probabilities.
  ```bash
  python run.py highestProbSpans
  ```

- **Feeder Impact**: Calculates customer impact statistics for specific feeders.
  ```bash
  python run.py feederInfo
  ```

---

### Step 4: Outputs

Processed data and results are saved in the `data/processed/` folder. Key outputs include:
- `merged_weather_data.csv`: Processed weather data.
- `weather_station_psps.csv`: PSPS probabilities for weather stations.
- `top_psps_stations.csv`: High-risk PSPS stations.
- `merged_vri_conductor_psps.csv`: Merged VRI and conductor data.
- `network_graph.json`: Directed graph of spans.
- `associated_weather_station_to_span.csv`: Weather stations linked to spans.
- `span_probabilities.csv`: Probabilities for spans.
- `spans_with_highest_probabilities.csv`: Spans with the highest probabilities.
- `feeder_impact_statistics.json`: Customer impact statistics for a specific feeder.

--- 

### Step 5: Verify Results

1. Use the `.csv` outputs for further analysis and visualization.
2. Open the `network_graph.json` file to review the span network.
3. Review `feeder_impact_statistics.json` for insights into customer impact for specific feeders.

--- 