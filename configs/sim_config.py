racks = 6
compute_nodes = 256
compute_node_capacity = 256
memory_node_capacity = 1024
memory_granularity = 1
memory_nodes = range(2, 41) # varying memory nodes
# memory_nodes = [10]
timeout_threshold = 0

# algorithms = ['sjf', 'fcfs', 'lsf', 'wfp3', 'unicep', 'f1', 'fair', 'fm', 'fm2', 'fm3'] # varying algorithms
# algorithms = ['fair', 'mratio']
algorithms = ['f1']
# allocation_funcs = ['system_balance', 'system_random', 'rack_balance', 'rack_random', 'rack_memory_aware']
# allocation_funcs = ['rack_balance', 'rack_random', 'rack_memory_aware']
allocation_funcs = ['rack_balance']


disaggregation = True
backfill = True
time_series = True

metric_folder = f'4d/4d_{algorithms[0]}_{allocation_funcs[0]}'
