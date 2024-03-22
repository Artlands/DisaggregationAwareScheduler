# racks = 6
# compute_nodes = 128
# compute_node_capacity = 256
# memory_node_capacity = 1024
# memory_node_capacity = 0
# memory_nodes = range(5, 36) # varying memory nodes
# memory_nodes = [0]
# slowdown_factor = -1
# algorithms = ['sjf', 'fcfs', 'lsf', 'wfp3', 'unicep', 'f1', 'fair', 'fm3'] # varying algorithms
# allocation_funcs = ['system_balance', 'system_random', 'rack_balance', 'rack_random', 'rack_memory_aware']
# allocation_funcs = ['rack_balance', 'system_balance']
# disaggregation = False

# For juelich
racks = 20
compute_nodes = 48
compute_node_capacity = 64
memory_node_capacity = 1024
memory_granularity = 1
memory_nodes = [5]
slowdown_factor = [i/20 for i in range(0, 21, 1)] # varying performance slowdown factor
timeout_threshold = 0
allocation_funcs = ['rack_balance']
disaggregation = True
backfill = True
time_series = False

algorithms = ['fcfs']
metric_folder = f'slowdown_{compute_node_capacity}_juelich_march6/{algorithms[0]}'
