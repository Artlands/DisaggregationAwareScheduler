import os
import sys
import yaml

# import another python file in the different directory
sys.path.insert(0, './configs/')
from sim_config import *

# Remove the existing config files in the directory
for filename in os.listdir('./configs/cluster/gen'):
  os.remove(f'./configs/cluster/gen/{filename}')
    

for mem_node in memory_nodes:
  for algo in algorithms:
    for alloc in allocation_funcs:
      if slowdown_factor:
        for s_factor in slowdown_factor:
          config = {
            'monitor': True,
            'node_status': False,
            'job_status': False,
            'raw_id': False,
            'racks': racks,
            'compute_nodes': compute_nodes,
            'memory_nodes': mem_node,
            'compute_node_capacity': compute_node_capacity,
            'memory_node_capacity': memory_node_capacity,
            'memory_granularity': memory_granularity,
            'offset': 0,
            'number': 0,
            'algorithm': algo,
            'allocation_func': alloc,
            'slowdown_factor': s_factor,
            'disaggregation': disaggregation,
            'backfill': backfill,
            'timeout_threshold': timeout_threshold,
            'metric_folder': metric_folder,
            'time_series': time_series,
          }
      
          config_file_name = f'./configs/cluster/gen/{algo}_{alloc}_bf_{mem_node}_{s_factor}.yaml'
          print(f'Generating {config_file_name}')
          
          with open(config_file_name, 'w') as f:
            yaml.dump(config, f)
      else:
        config = {
          'monitor': True,
          'node_status': False,
          'job_status': False,
          'raw_id': False,
          'racks': racks,
          'compute_nodes': compute_nodes,
          'memory_nodes': mem_node,
          'compute_node_capacity': compute_node_capacity,
          'memory_node_capacity': memory_node_capacity,
          'memory_granularity': memory_granularity,
          'offset': 0,
          'number': 0,
          'algorithm': algo,
          'allocation_func': alloc,
          'slowdown_factor': -1,
          'disaggregation': disaggregation,
          'backfill': backfill,
          'timeout_threshold': timeout_threshold,
          'metric_folder': metric_folder,
          'time_series': time_series,
        }
    
        config_file_name = f'./configs/cluster/gen/{algo}_{alloc}_bf_{mem_node}.yaml'
        print(f'Generating {config_file_name}')
        
        with open(config_file_name, 'w') as f:
          yaml.dump(config, f)
