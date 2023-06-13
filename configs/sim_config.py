compute_nodes = 200
compute_node_capacity = 256
memory_node_capacity = 1024
memory_granularity = 2
memory_nodes = range(1, 41) # varying memory nodes
# memory_nodes = [20]

disaggregation = True
rack_scale = False

# algorithms = ['sjf', 'fcfs', 'lsf', 'wfp3', 'unicep', 'f1'] # varying algorithms
algorithms = ['f1']
backfill = True

metric_folder = 'conf_compare_sys'