import os
import yaml

rack_size = 240
memory_nodes = [10, 20, 50, 100] # varying memory nodes
algorithms = ['sjf', 'fcfs', 'lsf', 'wfp3', 'unicep', 'f1', 'system_scale', 'rack_scale'] # varying algorithms
backfill = True


# Remove the existing config files in the directory
for filename in os.listdir('../configs/cluster/gen'):
    os.remove(f'../configs/cluster/gen/{filename}')
    

for mem_node in memory_nodes:
    for algo in algorithms:
        config = {
            'monitor': True,
            'node_status': False,
            'job_status': False,
            'raw_id': False,
            'disaggregation': True,
            'racks': 6,
            'compute_nodes': rack_size - mem_node,
            'memory_nodes': mem_node,
            'compute_node_capacity': 256,
            'memory_node_capacity': 1024,
            'memory_granularity': 2,
            'offset': 0,
            'number': 0,
            'algorithm': algo,
            'backfill': backfill
        }
        
        config_file_name = f'../configs/cluster/gen/{algo}_bf_{mem_node}.yaml'
        
        with open(config_file_name, 'w') as f:
            yaml.dump(config, f)
