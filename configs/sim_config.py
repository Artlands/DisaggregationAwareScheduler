# =========== For Perlmutter ===========
monitor     = True    # Monitor the simulation and write the accounting data to files
node_status = False   # Print the node status to the console
job_status  = True   # Print the job status to the console
raw_id      = False   # Use the raw job id
job_trace   = 'job_configs_lbnl.csv'

# ==== Cluster configuration ====
total_memory_per_rack = 40 * 1024  # Total memory capacity per rack in GB
memory_granularity    = 1       # Memory granularity in GB
racks                 = 6       # Number of racks
compute_nodes         = 256     # Number of compute nodes per rack
memory_node_capacity  = 0    # Memory capacity of each memory node in GB

compute_node_capacities = [512]      # Memory capacity of each compute node in GB
# compute_node_capacities = [16 * i for i in range(1, 9)]     # Memory capacity of each compute node in GB
# memory_nodes            = None
memory_nodes            = [0]    # Number of memory nodes per rack
# memory_nodes            = range(4, 49, 4)    # Number of memory nodes per rack
slowdown_factor         = [-1]    # Varying performance slowdown factor

# ==== Job trace configuration ====
offset            = 0           # Offset of the job trace
number            = 10           # Number of jobs to simulate, 0 means all jobs
warm_up_threshold = 0        # Number of jobs to warm up the system
timeout_threshold = 0           # Timeout threshold for jobs in the waiting queue
time_series       = False       # Job trace is time series or not

# ==== Scheduler configuration ====
# disaggregation   = True                   # Disaggregation is enabled or not
disaggregation   = False
backfill         = True                   # Backfill is enabled or not
# algorithms       = ['sjf', 'fcfs', 'wfp3', 'f1', 'fair', 'fm3']        # Job scheduling algorithms, available algorithms include: 'sjf', 'fcfs', 'wfp3', 'f1', 'fair', 'fm3'
algorithms       = ['fcfs']
allocation_funcs = ['system_balance']  # Varying resource allocation algorithms, available algorithms include: 'system_balance', 'system_random', 'rack_balance', 'rack_random', 'rack_memory_aware'
# allocation_funcs = ['system_balance', 'system_random', 'rack_balance', 'rack_random', 'rack_memory_aware']
metric_folder    = f'/perlmutter-64/baseline'   # Folder to store the accounting data

# # # =========== For Juelich ===========
# monitor     = True    # Monitor the simulation and write the accounting data to files
# node_status = False   # Print the node status to the console
# job_status  = False   # Print the job status to the console
# raw_id      = False   # Use the raw job id
# job_trace   = 'job_configs_juelich.csv'

# # ==== Cluster configuration ====
# total_memory_per_rack   = 8 * 1024  # Total memory capacity per rack in GB
# memory_granularity      = 1               # Memory granularity in GB
# racks                   = 20              # Number of racks
# compute_nodes           = 48              # Number of compute nodes per rack
# memory_node_capacity    = 1024            # Memory capacity of each memory node in GB

# compute_node_capacities = [64]              # Memory capacity of each compute node in GB
# # compute_node_capacities = [16 * i for i in range(1, 33)]
# # memory_nodes            = None             # Number of memory nodes per rack
# memory_nodes            = range(2, 25, 2)   # Number of memory nodes per rack
# # memory_nodes            = [0]    # Number of memory nodes per rack
# slowdown_factor         = [-1]            # Varying performance slowdown factor

# # ==== Job trace configuration ====
# offset            = 0           # Offset of the job trace
# number            = 0           # Number of jobs to simulate, 0 means all jobs
# warm_up_threshold = 3000        # Number of jobs to warm up the system
# timeout_threshold = 0           # Timeout threshold for jobs in the waiting queue 
# time_series       = False       # Job trace is time series or not

# # ==== Scheduler configuration ====
# disaggregation   = True                   # Disaggregation is enabled or not
# backfill         = True                   # Backfill is enabled or not
# algorithms       =['fm3']
# # algorithms       = ['sjf', 'fcfs', 'wfp3', 'f1', 'fair', 'fm3']        # Job scheduling algorithms, available algorithms include: 'sjf', 'fcfs', 'wfp3', 'f1', 'fair', 'fm3'
# allocation_funcs = ['rack_balance']       # Varying resource allocation algorithms, available algorithms include: 'system_balance', 'system_random', 'rack_balance', 'rack_random', 'rack_memory_aware'
# # allocation_funcs = ['system_balance', 'system_random', 'rack_balance', 'rack_random', 'rack_memory_aware']  # Varying resource allocation algorithms, available algorithms include: 'system_balance', 'system_random', 'rack_balance', 'rack_random', 'rack_memory_aware'
# metric_folder    = '/juelich-64-new/scope-rack'   # Folder to store the accounting data

