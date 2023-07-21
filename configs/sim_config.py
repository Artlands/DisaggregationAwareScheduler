racks = 6
compute_nodes = 200
compute_node_capacity = 256
memory_node_capacity = 1024
memory_granularity = 1
# memory_nodes = range(2, 41) # varying memory nodes
memory_nodes = [10]
timeout_threshold = 0

# algorithms = ['sjf', 'fcfs', 'lsf', 'wfp3', 'unicep', 'f1', 'fair'] # varying algorithms
# algorithms = ['fair', 'mratio']
algorithms = ['mratio']
# alloction_funcs = ['random_rack_aware', 'remote_memory_aware', 'rack_scale', 'system_scale']
allocation_funcs = ['remote_memory_aware']

disaggregation = True
backfill = True
time_series = True

metric_folder = '4d_rack_comp/4d_mratio(fair)_rack'