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
  def __init__(self, node_config):
    self.id = node_config.id
    self.allocated = node_config.allocated
    self.allocated_memory = 0
    self.memory_capacity = node_config.memory_capacity

  def attach(self, rack, cluster):
    self.rack = rack
    self.cluster = cluster

  def resource_round_up(self, x, base=4):
    return base * (int(math.ceil(x/base)))

  @property
  def free_memory(self):
    return self.memory_capacity - self.allocated_memory


class ComputeNode(Node):
  def __init__(self, node_config):
    Node.__init__(self, node_config)
    self.ntype = 'compute'
    self.job = None

  def run_job(self, job):
    self.allocated_memory += self.resource_round_up(job.local_memory)
    self.allocated = True
    self.job = job

  def stop_job(self, job):
    self.allocated_memory -= self.resource_round_up(job.local_memory)
    self.allocated = False
    self.job = None
  

class MemoryNode(Node):
  def __init__(self, node_config):
    Node.__init__(self, node_config)
    self.ntype = 'memory'
    self.jobs = []

  def allocate_memory(self, job):
    self.allocated_memory += self.resource_round_up(job.remote_memory)
    self.jobs.append(job)

  def deallocate_memory(self, job):
    self.allocated_memory -= self.resource_round_up(job.remote_memory)
    self.jobs.remove(job)