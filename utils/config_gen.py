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
    config = {
      'monitor': True,
      'node_status': False,
      'job_status': False,
      'raw_id': False,
      'disaggregation': disaggregation,
      'rack_scale': rack_scale,
      'racks': 6,
      'compute_nodes': compute_nodes,
      'memory_nodes': mem_node,
      'compute_node_capacity': compute_node_capacity,
      'memory_node_capacity': memory_node_capacity,
      'memory_granularity': memory_granularity,
      'offset': 0,
      'number': 0,
      'algorithm': algo,
      'backfill': backfill,
      'timeout_threshold': timeout_threshold,
      'warmup_threshold': 5000,
      'metric_folder': metric_folder
    }
    
    config_file_name = f'./configs/cluster/gen/{algo}_bf_{mem_node}.yaml'
    print(f'Generating {config_file_name}')
    
    with open(config_file_name, 'w') as f:
      yaml.dump(config, f)
