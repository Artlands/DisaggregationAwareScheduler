import os
import sys
import yaml

# import another python file in the different directory
sys.path.insert(0, './configs/')
from sim_config import *

# Remove the existing config files in the directory
for filename in os.listdir('./configs/cluster/gen'):
  os.remove(f'./configs/cluster/gen/{filename}')
    

for compute_node_capacity in compute_node_capacities:
  if memory_nodes:
    for mem_node in memory_nodes:
      for algo in algorithms:
        for alloc in allocation_funcs:
          config = {
            'monitor': monitor,
            'node_status': node_status,
            'job_status': job_status,
            'raw_id': raw_id,
            'racks': racks,
            'compute_nodes': compute_nodes,
            'memory_nodes': mem_node,
            'compute_node_capacity': compute_node_capacity,
            'memory_node_capacity': memory_node_capacity,
            'memory_granularity': memory_granularity,
            'offset': offset,
            'number': number,
            'algorithm': algo,
            'allocation_func': alloc,
            'disaggregation': disaggregation,
            'backfill': backfill,
            'timeout_threshold': timeout_threshold,
            'warm_up_threshold': warm_up_threshold,
            'metric_folder': metric_folder,
            'time_series': time_series,
            'job_trace': job_trace,
          }
          if slowdown_factor:
            for s_factor in slowdown_factor:
              config.update({'slowdown_factor': s_factor})
              config_file_name = f'./configs/cluster/gen/{algo}_{alloc}_bf_{mem_node}_{s_factor}.yaml'
              print(f'Generating {config_file_name}')
              
              with open(config_file_name, 'w') as f:
                yaml.dump(config, f)
          else:
            config.update({'slowdown_factor': -1})
            config_file_name = f'./configs/cluster/gen/{algo}_{alloc}_bf_{mem_node}.yaml'
            print(f'Generating {config_file_name}')
            
            with open(config_file_name, 'w') as f:
              yaml.dump(config, f)
  else:
    mem_node = round((total_memory_per_rack - compute_node_capacity*compute_nodes)/memory_node_capacity)
    for algo in algorithms:
      for alloc in allocation_funcs:
        config = {
          'monitor': monitor,
          'node_status': node_status,
          'job_status': job_status,
          'raw_id': raw_id,
          'racks': racks,
          'compute_nodes': compute_nodes,
          'memory_nodes': mem_node,
          'compute_node_capacity': compute_node_capacity,
          'memory_node_capacity': memory_node_capacity,
          'memory_granularity': memory_granularity,
          'offset': offset,
          'number': number,
          'algorithm': algo,
          'allocation_func': alloc,
          'disaggregation': disaggregation,
          'backfill': backfill,
          'timeout_threshold': timeout_threshold,
          'warm_up_threshold': warm_up_threshold,
          'metric_folder': metric_folder,
          'time_series': time_series,
          'job_trace': job_trace,
        }
        if slowdown_factor:
          for s_factor in slowdown_factor:
            config.update({'slowdown_factor': s_factor})
            config_file_name = f'./configs/cluster/gen/{algo}_{alloc}_bf_{mem_node}_{s_factor}.yaml'
            print(f'Generating {config_file_name}')
            
            with open(config_file_name, 'w') as f:
              yaml.dump(config, f)
        else:
          config.update({'slowdown_factor': -1})
          config_file_name = f'./configs/cluster/gen/{algo}_{alloc}_bf_{mem_node}.yaml'
          print(f'Generating {config_file_name}')
          
          with open(config_file_name, 'w') as f:
            yaml.dump(config, f)
