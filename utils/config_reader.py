import os
import yaml
import pandas as pd
from operator import attrgetter
from core.job import JobConfig
from algorithms.sjf import ShortestJobFirst
from algorithms.fcfs import FirstComeFirstServe
from algorithms.lsf import LeastScaleFirst
from algorithms.wfp3 import WFP3
from algorithms.unicep import UNICEP
from algorithms.f1 import F1
from algorithms.fair import Fair
from algorithms.mratio import Mratio
from algorithms.fm import FM
from algorithms.fm2 import FM2
from algorithms.fm3 import FM3
from algorithms.common import *
from utils.utils import interpolate

class CSVReader(object):
  def __init__(self, filename, cluster_config):
    print(f'Loading job configurations from {filename}')
    self.filename = filename
    self.offset = 0
    self.number = 0
    self.time_series = cluster_config.time_series
    
    if cluster_config.offset > 0:
      self.offset = cluster_config.offset
    if cluster_config.number > 0:
      self.number = cluster_config.number
      
    df = pd.read_csv(self.filename)
    
    if self.time_series:
      df['memory'] = df['memory'].apply(eval)
      df['memory'] = df.apply(lambda x: interpolate(x['memory'], x['duration']), axis=1)

    job_configs = []
    for i in range(len(df)):
      series = df.iloc[i]
      jobid = series.jobid
      submit = series.submit
      duration = series.duration
      nnodes = series.nnodes
      if self.time_series:
        max_memory = series.max_memory
        memory = series.memory
      else:
        max_memory = series.memory
        memory = []

      job_configs.append(JobConfig(jobid, submit, nnodes, max_memory, memory, duration))

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
    self.metric_folder = 'metrics'                  # folder of the monitoring metric files
    self.cluster_state_file = None                  # cluster state file
    self.jobs_summary_file = None                   # jobs summary file
    self.node_status = False                        # print node status when running or stopping jobs
    self.job_status = False                         # print job status when running or stopping jobs
    self.raw_id = False                             # use raw job IDs in job configurations
    self.offset = 0                                 # offset in the job configuration file
    self.number = 0                                 # number of jobs to be loaded
    self.total_racks = 6                            # total number of racks
    self.compute_nodes_per_rack = 200               # number of compute nodes in each rack
    self.memory_nodes_per_rack = 0                  # number of memory nodes in each rack
    self.memory_node_memory_capacity = 512          # memory capacity of the compute node (in GB)
    self.compute_node_memory_capacity = 0           # memory capacity of the memory node (in GB)
    self.memory_granularity = 1                     # memory allocation granularity (in GB)
    self.algorithm = FirstComeFirstServe()          # default scheduler algorithm
    self.allocation_func = system_balance_allocation  # default allocation function
    self.slowdown_factor = None                     # slowdown factor
    self.disaggregation = False                     # disaggregation option
    self.backfill = True                            # backfill option
    self.timeout_threshold = 36000                  # timeout threshold
    self.valid_algorithms = self.get_valid_algorithms
    self.valid_allocations = ['system_balance', 'system_random', 'rack_balance', 'rack_random', 'rack_memory_aware']
    self.time_series = False
    
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
      elif self.config['algorithm'] == 'lsf':
        self.algorithm = LeastScaleFirst()
      elif self.config['algorithm'] == 'wfp3':
        self.algorithm = WFP3()
      elif self.config['algorithm'] == 'unicep':
        self.algorithm = UNICEP()
      elif self.config['algorithm'] == 'f1':
        self.algorithm = F1()
      elif self.config['algorithm'] == 'fair':
        self.algorithm = Fair()
      elif self.config['algorithm'] == 'mratio':
        self.algorithm = Mratio()
      elif self.config['algorithm'] == 'fm':
        self.algorithm = FM()
      elif self.config['algorithm'] == 'fm2':
        self.algorithm = FM2()
      elif self.config['algorithm'] == 'fm3':
        self.algorithm = FM3()
      else:
        print(f'Invalid algorithm name. Please choose one from {self.valid_algorithms}.')
        exit(1)
    
    # load allocation function
    if 'allocation_func' in self.config:
      if self.config['allocation_func'] == 'system_balance':
        self.allocation_func = system_balance_allocation
      elif self.config['allocation_func'] == 'system_random':
        self.allocation_func = system_random_allocation
      elif self.config['allocation_func'] == 'rack_balance':
        self.allocation_func = rack_balance_allocation
      elif self.config['allocation_func'] == 'rack_random':
        self.allocation_func = rack_random_allocation
      elif self.config['allocation_func'] == 'rack_memory_aware':
        self.allocation_func = rack_memory_aware_allocation
      else:
        print(f'Invalid allocation function name. Please choose one from {self.valid_allocations}.')
        exit(1)
        
    if 'slowdown_factor' in self.config:
      self.slowdown_factor = self.config['slowdown_factor']
    
    if 'backfill' in self.config:
      self.backfill = self.config['backfill']
      
    if 'timeout_threshold' in self.config:
      self.timeout_threshold = self.config['timeout_threshold']
      
    if 'time_series' in self.config:
      self.time_series = self.config['time_series']
        
    # update monitoring option
    if 'monitor' in self.config:
      self.monitor = self.config['monitor']
      
    if 'metric_folder' in self.config:
      self.metric_folder = self.config['metric_folder']
      
    # update offset and number
    if 'offset' in self.config:
      self.offset = self.config['offset']
    if 'number' in self.config:
      self.number = self.config['number']
      
    if self.monitor:
      # if ./monitoring/{self.metric_folder} does not exist, create it
      if not os.path.exists(f"./monitoring/{self.metric_folder}"):
        os.makedirs(f"./monitoring/{self.metric_folder}")
      
      if not self.slowdown_factor:
        self.cluster_state_file = f"./monitoring/{self.metric_folder}/cluster_state_" \
                                + "J" + str(self.offset) \
                                + "-" + str(self.number) \
                                + "_C" + str(self.compute_nodes_per_rack)  \
                                + "-" + str(self.compute_node_memory_capacity) + "GB_" \
                                + "M" + str(self.memory_nodes_per_rack) \
                                + "-" + str(self.memory_node_memory_capacity) + "GB_"\
                                + self.config['algorithm'] \
                                + "-" + self.config['allocation_func'] \
                                + "_BF-" + str(self.backfill).lower() \
                                + ".json"
        self.jobs_summary_file = f"./monitoring/{self.metric_folder}/job_summary_" \
                                + "J" + str(self.offset) \
                                + "-" + str(self.number) \
                                + "_C" + str(self.compute_nodes_per_rack)  \
                                + "-" + str(self.compute_node_memory_capacity) + "GB_" \
                                + "M" + str(self.memory_nodes_per_rack) \
                                + "-" + str(self.memory_node_memory_capacity) + "GB_"\
                                + self.config['algorithm'] \
                                + "-" + self.config['allocation_func'] \
                                + "_BF-" + str(self.backfill).lower() \
                                + ".json"
      else:
        self.cluster_state_file = f"./monitoring/{self.metric_folder}/cluster_state_" \
                                + "J" + str(self.offset) \
                                + "-" + str(self.number) \
                                + "_C" + str(self.compute_nodes_per_rack)  \
                                + "-" + str(self.compute_node_memory_capacity) + "GB_" \
                                + "M" + str(self.memory_nodes_per_rack) \
                                + "-" + str(self.memory_node_memory_capacity) + "GB_"\
                                + self.config['algorithm'] \
                                + "-" + self.config['allocation_func'] \
                                + "_BF-" + str(self.backfill).lower() \
                                + "_SF-" + str(self.slowdown_factor) \
                                + ".json"
        self.jobs_summary_file = f"./monitoring/{self.metric_folder}/job_summary_" \
                                + "J" + str(self.offset) \
                                + "-" + str(self.number) \
                                + "_C" + str(self.compute_nodes_per_rack)  \
                                + "-" + str(self.compute_node_memory_capacity) + "GB_" \
                                + "M" + str(self.memory_nodes_per_rack) \
                                + "-" + str(self.memory_node_memory_capacity) + "GB_"\
                                + self.config['algorithm'] \
                                + "-" + self.config['allocation_func'] \
                                + "_BF-" + str(self.backfill).lower() \
                                + "_SF-" + str(self.slowdown_factor) \
                                + ".json"
  
  @property                        
  def get_valid_algorithms(self):
    folder_path = "./algorithms"
    default_files = ['__init__.py', 'common.py']
    files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    valid_algorithms = [f.replace('.py', '') for f in files if f not in default_files]
    return valid_algorithms

          