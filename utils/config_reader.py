import yaml
import random
import pandas as pd
from operator import attrgetter
from core.job import JobConfig
from algorithms.backfill import Backfill
from algorithms.sjf import ShortestJobFirst
from algorithms.fcfs import FirstComeFirstServe
from algorithms.rack_aware import RackAware
from algorithms.rack_disa import RackDisa


class CSVReader(object):
  def __init__(self, filename, cluster_config):
    print(f'Loading job configurations from {filename}')
    self.filename = filename
    self.offset = 0
    self.number = 0
    if cluster_config.offset > 0:
      self.offset = cluster_config.offset
    if cluster_config.number > 0:
      self.number = cluster_config.number
      
    df = pd.read_csv(self.filename)

    job_configs = []
    for i in range(len(df)):
      series = df.iloc[i]
      jobid = series.jobid
      submit = series.submit
      nnodes = series.nnodes
      memory = series.memory
      duration = series.duration
      if 'priority' in series:
        priority = series.priority
      else:
        priority = random.random()

      job_configs.append(JobConfig(jobid, submit, nnodes, memory, duration, priority))

    job_configs.sort(key=attrgetter('submit'))

    self.job_configs = job_configs
    self.total_number = len(df)

  def generate(self):
    print(f'Generating jobs')
    if self.number <= 0:
      self.number = self.total_number
    self.number = self.number if self.offset + self.number < len(self.job_configs) else len(self.job_configs) - self.offset
    ret = self.job_configs[self.offset: self.offset + self.number]
    the_first_job_config = ret[0]
    submit_base = the_first_job_config.submit

    for job_config in ret:
      job_config.submit -= submit_base

    print(f'Total number of jobs: {len(ret)}')

    return ret
  
class ClusterConfigReader(object):
  def __init__(self, filename):
    print(f'Loading cluster configuration from {filename}')
    self.filename = filename
    self.monitor = True                             # generate monitoring files
    self.cluster_state_file = None                  # cluster state file
    self.jobs_summary_file = None                   # jobs summary file
    self.node_status = False                        # print node status when running or stopping jobs
    self.job_status = False                         # print job status when running or stopping jobs
    self.raw_id = False                             # use raw job IDs in job configurations
    self.offset = 0                                 # offset in the job configuration file
    self.number = 0                                 # number of jobs to be loaded
    self.disaggregateion = False                    # disaggregation option
    self.total_racks = 6                            # total number of racks
    self.compute_nodes_per_rack = 240               # number of compute nodes in each rack
    self.memory_nodes_per_rack = 0                  # number of memory nodes in each rack
    self.memory_node_memory_capacity = 512          # memory capacity of the compute node (in GB)
    self.compute_node_memory_capacity = 0           # memory capacity of the memory node (in GB)
    self.memory_granularity = 2                     # memory allocation granularity (in GB)
    self.algorithm = FirstComeFirstServe() # default scheduler algorithm
    self.valid_algorithms = ['sjf', 'fcfs', 'backfill', 'rack_aware', 'rack_disa']
    
    with open(self.filename, 'r') as f:
      try:
        self.config = yaml.safe_load(f)
      except yaml.YAMLError as exc:
        # throw exception and terminate the program
        print(exc)
        exit(1)
    
    # update status option
    if 'node_status' in self.config:
      self.node_status = self.config['node_status']
      
    if 'job_status' in self.config:
      self.job_status = self.config['job_status']
      
    # update raw_id option
    if 'raw_id' in self.config:
      self.raw_id = self.config['raw_id']
      
    # update disaggregation option
    if 'disaggregation' in self.config:
      self.disaggregation = self.config['disaggregation']      
        
    # update default values
    if 'racks' in self.config:
      self.total_racks = self.config['racks']
      
    if 'compute_nodes' in self.config:
      self.compute_nodes_per_rack = self.config['compute_nodes']
      
    if 'memory_nodes' in self.config:
      self.memory_nodes_per_rack = self.config['memory_nodes']
    
    if 'compute_node_capacity' in self.config:
      self.compute_node_memory_capacity = self.config['compute_node_capacity']
    
    if 'memory_node_capacity' in self.config:
      self.memory_node_memory_capacity = self.config['memory_node_capacity']
      
    if 'memory_granularity' in self.config:
      self.memory_granularity = self.config['memory_granularity']
      
    # Sanity check
    if self.disaggregation:
      try:
        assert self.memory_nodes_per_rack > 0, 'Disaggregation option is enabled, but the number of memory nodes is 0.'
        assert self.memory_node_memory_capacity > 0, 'Disaggregation option is enabled, but the memory node capacity is 0.'
        assert self.memory_granularity > 0, 'Disaggregation option is enabled, but the memory granularity is 0.'
      except AssertionError as e:
        print(e)
        exit(1)

    # load algorithm
    if 'algorithm' in self.config:
      if self.config['algorithm'] == 'sjf':
        self.algorithm = ShortestJobFirst()
      elif self.config['algorithm'] == 'fcfs':
        self.algorithm = FirstComeFirstServe()
      elif self.config['algorithm'] == 'backfill':
        self.algorithm = Backfill()
      elif self.config['algorithm'] == 'rack_aware':
        self.algorithm = RackAware()
      elif self.config['algorithm'] == 'rack_disa':
        self.algorithm = RackDisa()
      else:
        print(f'Invalid algorithm name. Please choose from {self.valid_algorithms}.')
        exit(1)
        
    # update monitoring option
    if 'monitor' in self.config:
      self.monitor = self.config['monitor']
      
    # update offset and number
    if 'offset' in self.config:
      self.offset = self.config['offset']
    if 'number' in self.config:
      self.number = self.config['number']
      
    if self.monitor:
      self.cluster_state_file = "./monitoring/cluster_state_" \
                              + "J" + str(self.offset) \
                              + "-" + str(self.number) \
                              + "_C" + str(self.compute_nodes_per_rack)  \
                              + "-" + str(self.compute_node_memory_capacity) + "GB_" \
                              + "M" + str(self.memory_nodes_per_rack) \
                              + "-" + str(self.memory_node_memory_capacity) + "GB_"\
                              + self.config['algorithm'] + ".json"
      self.jobs_summary_file = "./monitoring/job_summary_" \
                              + "J" + str(self.offset) \
                              + "-" + str(self.number) \
                              + "_C" + str(self.compute_nodes_per_rack)  \
                              + "-" + str(self.compute_node_memory_capacity) + "GB_" \
                              + "M" + str(self.memory_nodes_per_rack) \
                              + "-" + str(self.memory_node_memory_capacity) + "GB_"\
                              + self.config['algorithm'] + ".json"
          