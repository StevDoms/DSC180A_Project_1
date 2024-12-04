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
    ```

---

### Step 1: Set Up the Environment

1. Open a terminal and navigate to the project directory:
   ```bash
   cd /path/to/DSC180A_Q1_project
   ```

2. Run the following commands to set up the Conda environment:
   ```bash
   conda env create -f environment.yml
   conda activate geo_env
   ```
   Note if you are running in DSMLP you might get an instruction CondaError: Run 'conda init' before 'conda activate', if so run before conda activate geo_env:

   ```bash
   source ~/.bashrc
   ```

4. After the Conda environment is activated, you are prepared to run the project code.

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

- **Filter PSPS Stations**: Filters high-risk PSPS stations based on a threshold.
  ```bash
  python run.py filter
  ```

- **VRI and Conductor Merge**: Merges conductor and vegetation risk index (VRI) data.
  ```bash
  python run.py merge-vri
  ```

- **Analyze spans**: Builds a directed graph of spans for upstream/downstream analysis to perform span analysis and and calculate probabilities of each span.
  ```bash
  python run.py analyze spans
  ```

- **Feeder analysis**: Perform feeder analysis by exploring the annual customers affected for a given parent feeder id and predicting number of customers affected in 10 years.
  ```bash
  python run.py feeder_analysis
  ```

---

### Step 4: Outputs
We showed our expected outputs in proj1_notebook_results as print statements, feel free to use the notebooks to verify your results.