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
  # Configure cluster
  cluster = Cluster()
  total_racks = 6
  
  compute_nodes_per_rack = 12 # Configure Number of computer nodes
  memory_nodes_per_rack = 8   # Number of memory nodes
  
  compute_node_memory_capacity = 128 #GB
  memory_node_memory_capacity = 2048 #GB
  memory_granularity = 4 #GB  
  
  racks = []
  for _ in range(total_racks):
    node_configs = []
    for c in range(compute_nodes_per_rack):
      node_configs.append(NodeConfig(compute_node_memory_capacity, 'compute'))
    for m in range(memory_nodes_per_rack):
      node_configs.append(NodeConfig(memory_node_memory_capacity, 'memory'))

    rack = Rack()
    rack.add_nodes(node_configs, memory_granularity)
    racks.append(rack)
    
  cluster.add_racks(racks)
  
  # Disaggregation option
  disaggregation=True
  cluster.set_disaggregation(disaggregation)

  # Monitoring option
  monitor = False

  # Configure jobs
  total_jobs = 14110
  jobs_csv = './job_configs.csv'
  csv_reader = CSVReader(jobs_csv)
  job_configs = csv_reader.generate(5000, 1000)

  # Select scheduling algorithm
  # algorithm = RandomAlgorithm()
  algorithm = RackFirstAlgorithm()

  # Simulation environment
  env = simpy.Environment()

  # Job broker, submits job to the cluster
  job_broker = Broker(env, job_configs, raw_id=False)

  # Job scheduler
  scheduler = Scheduler(env, algorithm, disaggregation)

  # Set up file for storing the monitoring data
  if monitor:
    cluster_state_file = './monitoring/cluster_state_file_t.json'
    jobs_summary_file = './monitoring/jobs_summary_file_t.json'
  else:
    cluster_state_file = None
    jobs_summary_file = None

  # Create simulation for the current settings
  simulation = Simulation(env, cluster, job_broker, scheduler, cluster_state_file, jobs_summary_file)
  
  simulation.run()
  env.run()


if __name__ == "__main__":
  main()
