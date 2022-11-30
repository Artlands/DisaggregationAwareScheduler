import simpy

from core.node import NodeConfig
from core.cluster import Cluster
from core.broker import Broker
from core.scheduler import Scheduler
from core.simulation import Simulation

from utils.csv_reader import CSVReader
from algorithms.random import RandomAlgorithm

def main():
  # Configure nodes
  total_racks = 6
  nodes_per_rack = 256
  total_nodes = total_racks * nodes_per_rack
  memory_per_node = 512 # GB
  node_configs = [NodeConfig(memory_per_node) for i in range(total_nodes)]

  # Configure cluster
  cluster = Cluster()
  cluster.add_racks(node_configs)

  # Configure jobs
  total_jobs = 14110
  jobs_csv = './job_configs.csv'
  csv_reader = CSVReader(jobs_csv)
  job_configs = csv_reader.generate(2000, 100)

  # Select scheduling algorithm
  algorithm = RandomAlgorithm()

  # Simulation environment
  env = simpy.Environment()

  # Job broker, add job to the cluster
  job_broker = Broker(env, job_configs)

  # Job scheduler
  scheduler = Scheduler(env, algorithm)

  # Set up file for storing the monitoring data
  cluster_state_file = './monitoring/cluster_state_file.json'
  jobs_summary_file = './monitoring/jobs_summary_file.json'

  # Create simulation for the current settings
  simulation = Simulation(env, cluster, job_broker, scheduler, cluster_state_file, jobs_summary_file)

  simulation.run()
  env.run()


if __name__ == "__main__":
  main()
