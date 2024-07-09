import random
from algorithms.fcfs import FirstComeFirstServe
from algorithms.common import system_balance_allocation
from utils.utils import interpolate

from scipy.interpolate import interp1d

# The following values are based on the experimental results from PARSEC benchmarks
intra_rack_slowdown = [1.03935594e-01, 1.37285957e+00, 1.37771886e+00, 1.83561765e+00,
                       4.67776163e+00, 4.83442012e+00, 5.87674881e+00, 1.37825116e+01,
                       3.06614957e+01, 1.06645952e+02, 1.66840426e+02] 
inter_rack_slowdown = [  0.75210565,   1.96119392,   3.59254798,   5.37873038,
                        11.20019087,  12.49451142,  17.857027  ,  25.00913367,
                        52.94264714, 127.45616256, 318.95031442]
cdf = [0.09090909, 0.18181818, 0.27272727, 0.36363636, 0.45454545,
       0.54545455, 0.63636364, 0.72727273, 0.81818182, 0.90909091, 1.        ]

inverse_cdf_intra = interp1d(cdf, intra_rack_slowdown, kind='quadratic', fill_value='extrapolate')
inverse_cdf_inter = interp1d(cdf, inter_rack_slowdown, kind='quadratic', fill_value='extrapolate')


class Scheduler(object):
  def __init__(self, env, algorithm, allocation_func, slowdown_factor, 
               backfill, timeout_threshold,  warm_up_threshold, 
               time_series):
    print(f'Initializing scheduler')
    self.env = env
    self.algorithm = algorithm
    self.allocation_func = allocation_func
    self.slowdown_factor = slowdown_factor
    self.backfill = backfill
    self.time_series = time_series
    self.simulation = None
    self.cluster = None
    self.destroyed = False
    self.warm_up_jobs_count = 0
    self.warm_up_scheduler = FirstComeFirstServe()
    self.timeout_threshold = timeout_threshold
    self.warm_up_threshold = warm_up_threshold

  def attach(self,simulation):
    self.simulation = simulation
    self.cluster = simulation.cluster

  def make_decision(self):
    while True:
      # Check jobs in the waiting queue to see if they wait too long; this is
      # to prevent from simulation running forever in some cases
      for job in self.cluster.jobs_in_waiting_queue:
        wait_time = self.env.now - job.submit
        if self.timeout_threshold != 0 and wait_time > self.timeout_threshold:
          job.fail()
          self.cluster.add_failed_jobs(job, 'timeout')
          
      # Warm-up period, use first-come-first-serve and backfill to schedule jobs
      if self.warm_up_jobs_count < self.warm_up_threshold:
        job, compute_memory_node_tuples = self.warm_up_scheduler(self.cluster, self.env.now, 
                                                                 True, system_balance_allocation)
      else:
        # Get the warm-up time
        if self.warm_up_jobs_count == self.warm_up_threshold:
          self.cluster.warm_up_time = self.env.now
          
        # Schedule jobs using the specified algorithm
        job, compute_memory_node_tuples = self.algorithm(self.cluster, self.env.now, 
                                                         self.backfill, self.allocation_func)
        
      
      if (job == None):
        break
      else:
        # Calculate performance slowdown
        compute_nodes, memory_nodes = self.performance_slowdown(job, compute_memory_node_tuples)
        job.run(compute_nodes, memory_nodes)
        self.warm_up_jobs_count += 1
        
        
  def performance_slowdown(self, job, compute_memory_node_tuples):
    # Model the performance slowdown based on ratio of remote memory and the distance of the allocated compute nodes to the memory nodes.
    slowdown = 0
    cross_rack = False
    compute_nodes = []
    memory_nodes = []      # {'memory_node': MemoryNode, 'remote_memory': memory_allocated}
    memory_nodes_dict = {} # {'memory_node_id': {'memory_node': MemoryNode, 'remote_memory': memory_allocated} }
    for (c_node, m_node, remote_memory) in compute_memory_node_tuples:
      compute_nodes.append(c_node)
      # Aggregate remote memory
      if m_node:
        if m_node.id in memory_nodes_dict:
          memory_nodes_dict[m_node.id]['remote_memory'] += remote_memory
        else:
          memory_nodes_dict[m_node.id] = {'memory_node': m_node, 'remote_memory': remote_memory}
      
        # if the compute node and memory node are in different racks, set cross_rack to True
        c_node_rack = c_node.rack.id
        m_node_rack = m_node.rack.id
        if(c_node_rack != m_node_rack):
          cross_rack = True
    
    memory_nodes = list(memory_nodes_dict.values())
    
    if memory_nodes:
      # if self.time_series:
      #   # Remote memory ratio, the larger the greater slowdown. We consider the temporal characteristics of the memory
      #   rm_ratio = job.remote_memory_scale/job.total_memory_scale
      # else:
      rm_ratio = 1 - self.cluster.compute_node_memory_capacity/job.max_memory
      
      if self.slowdown_factor == -1:
        if cross_rack:
          base_slowdown = inverse_cdf_inter(job.randomness)
        else:
          base_slowdown = inverse_cdf_intra(job.randomness)
        if base_slowdown < 0:
          slowdown = 0
        slowdown = round(base_slowdown/100 * rm_ratio, 2)
      else:
        slowdown = self.slowdown_factor
    
    # Adjust job duration and slowdown
    job.slowdown = slowdown
    job.duration = int(job.duration * (slowdown + 1))
    
    # Adjust job memory records by interpolating the memory usage
    if slowdown > 0 and self.time_series:
      job.memory = interpolate(job.memory, job.duration)
    
    return compute_nodes, memory_nodes
  
  
  def get_truncated_normal(self, mean=0.22, sd=0.1, low=0, upp=0.55):
    number = random.normalvariate(mean, sd)
    while number < low or number > upp:
      number = random.normalvariate(mean, sd)
    return number
  

  def run(self):
    while not self.simulation.finished:
      self.make_decision()
      yield self.env.timeout(1)
    self.destroyed = True