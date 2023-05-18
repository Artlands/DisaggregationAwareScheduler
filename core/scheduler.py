import random
from scipy.stats import truncnorm

class Scheduler(object):
  def __init__(self, env, algorithm):
    print(f'Initializing scheduler')
    self.env = env
    self.algorithm = algorithm
    self.simulation = None
    self.cluster = None
    self.destroyed = False

  def attach(self,simulation):
    self.simulation = simulation
    self.cluster = simulation.cluster

  def make_decision(self):
    while True:
      job, compute_memory_node_tuples = self.algorithm(self.cluster, self.env.now)
      if (len(compute_memory_node_tuples) == 0) or (job == None):
        break
      else:
        # Calculate performance slowdown
        compute_nodes, memory_nodes = self.performance_slowdown(job, compute_memory_node_tuples)
        job.start(compute_nodes, memory_nodes)
        
  def performance_slowdown(self, job, compute_memory_node_tuples):
    # Model the performance slowdown based on ratio of remote memory and the distance of the allocated compute nodes to the memory nodes.
    slowdown = 1
    distance = 0
    compute_nodes = []
    memory_nodes = []      # {'memory_node': MemoryNode, 'remote_memory': memory_allocated}
    memory_nodes_dict = {} # { 'memory_node_id': {'memory_node': MemoryNode, 'remote_memory': memory_allocated} }
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
        if(c_node_rack != m_node_rack):
          distance = 1
        else:
          distance = 2
    
    memory_nodes = list(memory_nodes_dict.values())
    
    if memory_nodes:
      # Remote memory ratio, the larger the greater slowdown
      rm_ratio = 1 - self.cluster.compute_node_memory_capacity/job.memory
      # Distance ratio, the larger the greater slowdowns
      ds_ratio = distance
      
      base_slowdown = self.get_truncated_normal().rvs()
      
      # Calculate slowdown based on remote memory radio and distance ratio
      slowdown = round(base_slowdown * ds_ratio * (1 + rm_ratio), 2)
      job.slowdown = slowdown
    
    # Adjust job duration
    job.duration = int(job.duration * (slowdown + 1))
    
    return compute_nodes, memory_nodes
  
  def get_truncated_normal(self, mean=0.25, sd=0.1, low=0, upp=0.5):
    return truncnorm(
        (low - mean) / sd, (upp - mean) / sd, loc=mean, scale=sd)

  def run(self):
    while not self.simulation.finished:
      self.make_decision()
      yield self.env.timeout(1)
    self.destroyed = True