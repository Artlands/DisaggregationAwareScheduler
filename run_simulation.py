import simpy

from core.node import NodeConfig
from core.rack import Rack
from core.cluster import Cluster
from core.broker import Broker
from core.scheduler import Scheduler
from core.simulation import Simulation

from utils.csv_reader import CSVReader
from algorithms.random import RandomAlgorithm
from algorithms.rack_first import RackFirstAlgorithm


def main():
  # Monitoring option
  monitor = True    # Generate monitoring files
  status = False     # Print node status when running or stopping jobs
  raw_id = False    # Use raw job IDs in job configurations
  
  # Set up files for storing the monitoring data
  if monitor:
    cluster_state_file = './monitoring/7d_rack_first_cluster_state_file_perlmutter.json'
    jobs_summary_file = './monitoring/7d_rack_first_summary_file_perlmutter.json'
  else:
    cluster_state_file = None
    jobs_summary_file = None
  
  # Disaggregation option
  disaggregation=True
  
  # Scheduling algorithm options
  # algorithm = RandomAlgorithm()
  algorithm = RackFirstAlgorithm()
  
  # Cluster configuration
  cluster = Cluster(status)
  cluster.set_disaggregation(disaggregation)
  
  total_racks = 6                     # Total number of racks
  compute_nodes_per_rack = 230        # Number of computer nodes in each rack
  memory_nodes_per_rack = 10           # Number of memory nodes in each rack
  compute_node_memory_capacity = 128  # Memory capacity of the computer node (in GB)
  memory_node_memory_capacity = 2048  # memory capacity of the memory node (in GB)
  memory_granularity = 2              # Memory allocation granularity (in GB) 
  
  for _ in range(total_racks):
    node_configs = []
    for c in range(compute_nodes_per_rack):
      node_configs.append(NodeConfig(compute_node_memory_capacity, 'compute'))
    for m in range(memory_nodes_per_rack):
      node_configs.append(NodeConfig(memory_node_memory_capacity, 'memory'))

    rack = Rack()
    cluster.add_rack(rack)
    rack.add_nodes(node_configs, memory_granularity)

  # Loading jobs
  total_jobs = 14110
  # total_jobs = 12138
  jobs_csv = './configs/job_configs_7days.csv'
  csv_reader = CSVReader(jobs_csv)
  job_configs = csv_reader.generate()    # First parameter: starting point in the csv file
                                                   # Second parameter: total number of job records

  # Simulation environment
  env = simpy.Environment()

  # Job broker: submits job to the cluster
  job_broker = Broker(env, job_configs, raw_id)

  # Job scheduler
  scheduler = Scheduler(env, algorithm)

  # Create simulation for the current settings
  simulation = Simulation(env, cluster, job_broker, scheduler, cluster_state_file, jobs_summary_file)
  
  simulation.run()
  env.run()


if __name__ == "__main__":
  main()
