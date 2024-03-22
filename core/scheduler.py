import random
from algorithms.fcfs import FirstComeFirstServe
from utils.utils import interpolate

class Scheduler(object):
  def __init__(self, env, algorithm, allocation_func, slowdown_factor, 
               backfill, timeout_threshold, time_series):
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
    self.timeout_threshold = timeout_threshold

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
      
      # Schedule jobs
      job, compute_memory_node_tuples = self.algorithm(self.cluster, self.env.now, 
                                                        self.backfill, self.allocation_func)
      
      if (job == None):
        break
      else:
        # Calculate performance slowdown
        compute_nodes, memory_nodes = self.performance_slowdown(job, compute_memory_node_tuples)
        job.run(compute_nodes, memory_nodes)
        
  def performance_slowdown(self, job, compute_memory_node_tuples):
    # Model the performance slowdown based on ratio of remote memory and the distance of the allocated compute nodes to the memory nodes.
    slowdown = 0
    distance = 0
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
      
        # Calculate distance 
        # if the compute node and memory node are in the same rack, 
        # distance is 1, otherwise distance is 2
        c_node_rack = c_node.rack.id
        m_node_rack = m_node.rack.id
        if(c_node_rack == m_node_rack):
          distance = 1
        else:
          distance = 2
    
    memory_nodes = list(memory_nodes_dict.values())
    
    if memory_nodes:
      # if self.time_series:
      #   # Remote memory ratio, the larger the greater slowdown. We consider the temporal characteristics of the memory
      #   rm_ratio = job.remote_memory_scale/job.total_memory_scale
      # else:
      rm_ratio = 1 - self.cluster.compute_node_memory_capacity/job.max_memory
      
      # Distance ratio, the larger the greater slowdowns
      ds_ratio = distance
      
      if self.slowdown_factor == -1:
        base_slowdown = self.get_truncated_normal()
        # Calculate slowdown based on remote memory radio and distance ratio
        slowdown = round(base_slowdown * ds_ratio * (1 + rm_ratio), 2)
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
    # return truncnorm(
    #     (low - mean) / sd, (upp - mean) / sd, loc=mean, scale=sd)
    random.seed(34)
    number = random.normalvariate(mean, sd)
    while number < low or number > upp:
      number = random.normalvariate(mean, sd)
    return number
  

  def run(self):
    while not self.simulation.finished:
      self.make_decision()
      yield self.env.timeout(1)
    self.destroyed = True