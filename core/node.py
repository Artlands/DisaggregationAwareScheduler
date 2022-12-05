import math

class NodeConfig(object):
  idx = 0
  def __init__(self, memory_capacity, node_type):
    self.id = NodeConfig.idx
    self.ntype = node_type
    self.memory_capacity = memory_capacity
    self.allocated = False
    NodeConfig.idx += 1


class Node(object):
  def __init__(self, node_config, memory_granularity):
    self.id = node_config.id
    self.allocated = node_config.allocated
    self.allocated_memory = 0
    self.memory_capacity = node_config.memory_capacity
    self.memory_granularity = memory_granularity
    self.rack = None
    self.cluster = None

  def attach(self, rack, cluster):
    self.rack = rack
    self.cluster = cluster

  def resource_round_up(self, x):
    return self.memory_granularity * (int(math.ceil(x/self.memory_granularity)))

  @property
  def free_memory(self):
    return self.memory_capacity - self.allocated_memory


class ComputeNode(Node):
  def __init__(self, node_config, memory_granularity):
    Node.__init__(self, node_config, memory_granularity)
    self.ntype = 'compute'
    self.job = None

  def run_job(self, job):
    if self.cluster.status == True:
      print(f'Compute Node {self.rack.id}-{self.id} runs Job {job.id}')
    self.allocated_memory += self.resource_round_up(min(job.memory, self.memory_capacity))
    self.allocated = True
    self.job = job

  def stop_job(self, job):
    if self.cluster.status == True:
      print(f'Compute Node {self.rack.id}-{self.id} stops Job {job.id}')
    self.allocated_memory -= self.resource_round_up(min(job.memory, self.memory_capacity))
    self.allocated = False
    self.job = None
  

class MemoryNode(Node):
  def __init__(self, node_config, memory_granularity):
    Node.__init__(self, node_config, memory_granularity)
    self.ntype = 'memory'
    self.jobs = []

  def allocate_memory(self, job, remote_memory):
    if self.cluster.status == True:
      print(f'Memory Node {self.rack.id}-{self.id} allocates remote memory {self.resource_round_up(remote_memory)} GB for Job {job.id}')
    self.allocated_memory += self.resource_round_up(remote_memory)
    self.jobs.append(job)

  def deallocate_memory(self, job, remote_memory):
    if self.cluster.status == True:
      print(f'Memory Node {self.rack.id}-{self.id} deallocates remote memory {self.resource_round_up(remote_memory)} GB for Job {job.id}')
    self.allocated_memory -= self.resource_round_up(remote_memory)
    self.jobs.remove(job)