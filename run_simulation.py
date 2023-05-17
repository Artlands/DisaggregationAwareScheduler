import simpy

from core.cluster import Cluster
from core.broker import Broker
from core.scheduler import Scheduler
from core.simulation import Simulation

from utils.config_reader import CSVReader, ClusterConfigReader


def main():
  # Load cluster configuration
  cluster_config = ClusterConfigReader('./configs/cluster_config.yaml')
  
  # Setup monitoring options
  raw_id             = cluster_config.raw_id
  cluster_state_file = cluster_config.cluster_state_file
  jobs_summary_file  = cluster_config.jobs_summary_file
  
  # Setup algorithm
  algorithm = cluster_config.algorithm
  
  # Initialize cluster
  cluster = Cluster(cluster_config)

  # Loading jobs
  csv_reader = CSVReader('./configs/job_configs_3days.csv', cluster_config)
  job_configs = csv_reader.generate()

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
