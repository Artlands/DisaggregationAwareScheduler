# DisaggregationAwareScheduler

## About

**DisaggregationAwareScheduler** is a Python-based simulator designed for studying scheduling in HPC systems with disaggregated memory resources. It utilizes `simpy` as the discrete event simulation engine and supports user-defined system configurations, job traces, and scheduling algorithms.

## Preparation

### 1. Set Up a Virtual Environment

Before running any simulation, users need to set up a virtual environment and install the required dependencies.

```bash
# Create the virtual environment
python -m venv .venv

# Activate the virtual environment (Linux/macOS)
source .venv/bin/activate

# Activate the virtual environment (Windows)
.venv\Scripts\activate

# Install the required packages
pip install -r requirements.txt
```

### 2. Prepare a Job Trace File

Users must prepare a job trace file that will be submitted to the simulated cluster. The job trace file should contain the following fields:

- **jobid**: Job ID
- **submit**: Time when the job is submitted to the system
- **start**: Time when the job starts execution
- **end**: Time when the job finishes execution
- **nnodes**: Number of nodes requested by the job
- **duration**: Original job duration (`end - start`)
- **max_memory**: Maximum memory capacity per node used by the job

Example job trace files can be found in the `/configs/job/` folder, including traces collected from **Perlmutter (LBNL)** and **Juwels (Juelich)**.

## Simulation Configuration

DisaggregationAwareScheduler allows users to configure various system settings and scheduling policies using a Python-based configuration file.

### 1. Automatic Configuration Generation

The main configuration file is located at `/configs/sim_config.py`. Users can modify this file to customize system parameters and generate configuration files automatically.

After making changes, save the file and run:

```bash
make gen
```

This command will generate a series of configuration files, which will be saved in the `/configs/cluster/gen/` folder.

### 2. Manual Configuration

Users can also manually define their configurations using a YAML file. Below is an example configuration:

```yaml
algorithm: fm3                            # Job scheduling algorithm
allocation_func: rack_balance             # Resource allocation algorithm
backfill: true                            # Enable backfilling
compute_node_capacity: 64                 # Memory capacity of each compute node (GB)
compute_nodes: 48                         # Number of compute nodes per rack
disaggregation: true                      # Enable disaggregation
job_status: false                         # Do NOT print job status to the console
job_trace: job_configs_juelich.csv        # Job trace file name
memory_granularity: 1                     # Memory allocation granularity (GB)
memory_node_capacity: 1024                # Memory capacity of each memory node (GB)
memory_nodes: 2                           # Number of memory nodes per rack
metric_folder: /juelich-64/fm3            # Folder to store accounting data (under /monitoring/)
monitor: true                             # Enable monitoring and write accounting data to files
node_status: false                        # Do NOT print node status to the console
number: 0                                 # Number of jobs to simulate (0 means all jobs)
offset: 0                                 # Offset in the job trace
racks: 20                                 # Number of racks
raw_id: false                             # Do NOT use raw job IDs
slowdown_factor: -1                       # Use slowdown model to get slowdown factor
time_series: false                        # Job trace is NOT time-series
timeout_threshold: 0                      # Timeout threshold for jobs in the waiting queue
warm_up_threshold: 3000                   # Number of jobs to warm up the system
```

## Running the Simulation

To run the simulation, execute:

```bash
python run_simulation.py --cluster_config /path/to/configuration.yaml
```

Alternatively, you can use the provided bash script:

```bash
./run_sim.sh
```

This script runs simulations based on the configuration files generated in the `/configs/cluster/gen/` folder.

### Output

After the simulation completes, the accounting data will be saved in the user-specified folder for further analysis.

