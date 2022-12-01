import simpy

from core.node import NodeConfig
from core.rack import Rack
from core.cluster import Cluster
from core.broker import Broker
from core.scheduler import Scheduler
from core.simulation import Simulation

from utils.csv_reader import CSVReader
from algorithms.random import RandomAlgorithm


def main():
  # Configure cluster
  cluster = Cluster()
  total_racks = 6
  compute_node_memory_capacity = 512 #GB
  memory_node_memory_capacity = 2048 #GB
  racks = []
  for _ in range(total_racks):
    compute_node_config = NodeConfig(compute_node_memory_capacity, 'compute') 
    memory_node_config = NodeConfig(memory_node_memory_capacity, 'memory')
    node_configs = [compute_node_config for _ in range(256)] + [memory_node_config for _ in range(0)]
    rack = Rack()
    rack.add_nodes(node_configs)
    racks.append(rack)

  cluster.add_racks(racks)

  disaggregation=False

  # Configure jobs
  total_jobs = 14110
  jobs_csv = './job_configs.csv'
  csv_reader = CSVReader(jobs_csv)
  job_configs = csv_reader.generate(0, 100)

  # Select scheduling algorithm
  algorithm = RandomAlgorithm()

  # Simulation environment
  env = simpy.Environment()

  # Job broker, add job to the cluster
  job_broker = Broker(env, job_configs, compute_node_memory_capacity)

  # Job scheduler
  scheduler = Scheduler(env, algorithm, disaggregation)

  # Set up file for storing the monitoring data
  cluster_state_file = './monitoring/cluster_state_file.json'
  jobs_summary_file = './monitoring/jobs_summary_file.json'

  # Create simulation for the current settings
  # simulation = Simulation(env, cluster, job_broker, scheduler, cluster_state_file, jobs_summary_file)
  simulation = Simulation(env, cluster, job_broker, scheduler)

  simulation.run()
  env.run()


if __name__ == "__main__":
  main()
