racks = 6
compute_nodes = 256
compute_node_capacity = 256
memory_node_capacity = 1024

memory_granularity = 1
# memory_nodes = range(5, 36) # varying memory nodes
memory_nodes = [10]
# slowdown_factor = None
slowdown_factor = [i/20 for i in range(0, 21, 1)] # varying performance slowdown factor

timeout_threshold = 0

# algorithms = ['sjf', 'fcfs', 'lsf', 'wfp3', 'unicep', 'f1', 'fair', 'fm3'] # varying algorithms
# algorithms = ['fcfs', 'fair']
# algorithms = ['fair', 'mratio']
algorithms = ['fcfs']
# allocation_funcs = ['system_balance', 'system_random', 'rack_balance', 'rack_random', 'rack_memory_aware']
# allocation_funcs = ['rack_balance', 'system_balance']
allocation_funcs = ['rack_balance']


disaggregation = True
# backfill = False
backfill = True
time_series = True

# metric_folder = f'ipdps-256/4d_{algorithms[0]}_{allocation_funcs[0]}_sf'
metric_folder = f'slowdown_256/fcfs'
