import simpy
import argparse

from core.cluster import Cluster
from core.broker import Broker
from core.scheduler import Scheduler
from core.simulation import Simulation

from utils.config_reader import CSVReader, ClusterConfigReader


def main():
  # Parse arguments
  parser = argparse.ArgumentParser(description='Run simulation by specifying configuration files')
  parser.add_argument('--cluster_config', metavar='cluster_config.yaml', type=str, help='simulation configuration file')
  parser.add_argument('--job_config', metavar='job_config.csv', type=str, help='job configuration file')
  args = parser.parse_args()
  
  # Load cluster configuration
  if args.cluster_config:
    cluster_config = ClusterConfigReader(f'{args.cluster_config}')
  else:
    cluster_config = ClusterConfigReader('./configs/cluster/cluster_config.yaml')
  
  # Setup monitoring options
  raw_id             = cluster_config.raw_id
  cluster_state_file = cluster_config.cluster_state_file
  jobs_summary_file  = cluster_config.jobs_summary_file
  
  # Setup algorithm
  algorithm = cluster_config.algorithm
  allocation_func = cluster_config.allocation_func
  backfill  = cluster_config.backfill
  timeout_threshold = cluster_config.timeout_threshold
  time_series = cluster_config.time_series
  slowdown_factor = cluster_config.slowdown_factor

  # Loading jobs
  if args.job_config:
    csv_reader = CSVReader(f'{args.job_config}', cluster_config)
  else:
    csv_reader = CSVReader('./configs/job/job_configs_3days.csv', cluster_config)
  job_configs = csv_reader.generate()

  # Simulation environment
  env = simpy.Environment()
  
  # Initialize cluster
  cluster = Cluster(env, cluster_config)

  # Job broker: submits job to the cluster
  job_broker = Broker(env, job_configs, raw_id)

  # Job scheduler
  scheduler = Scheduler(env, algorithm, allocation_func, slowdown_factor, 
                        backfill, timeout_threshold, time_series)

  # Create simulation for the current settings
  simulation = Simulation(env, cluster, job_broker, scheduler, 
                          cluster_state_file, jobs_summary_file)
  
  simulation.run()
  env.run()


if __name__ == "__main__":
  main()
