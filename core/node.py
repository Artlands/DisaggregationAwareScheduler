import math


class NodeConfig(object):
  idx = 0
  def __init__(self, memory_capacity):
    self.memory_capacity = memory_capacity
    self.allocated = False
    self.id = NodeConfig.idx
    NodeConfig.idx += 1


class Node(object):
  def __init__(self, node_config):
    self.id = node_config.id
    self.memory_capacity = node_config.memory_capacity
    self.allocated_memory = 0
    self.allocated = node_config.allocated
    self.job = None
    self.shared_memory_jobs = []

  def attach(self, rack, cluster):
    self.rack = rack
    self.cluster = cluster

  def run_job(self, job):
    self.allocated_memory += self.__resource_round_up(job.memory)
    self.allocated = True
    self.job = job

  def stop_job(self, job):
    self.allocated_memory -= self.__resource_round_up(job.memory)
    self.allocated = False
    self.job = None

  # def allocate_shared_memory(self, job):
  #   self.allocated_memory += self.__resource_round_up(job.shared_memory)
  #   self.shared_memory_jobs.append(job)
  
  # def deallocated_shared_memory(self, job):
  #   self.allocated_memory -= self.__resource_round_up(job.shared_memory)
  #   self.shared_memory_jobs.remove(job)

  @property
  def free_memory(self):
    return self.memory_capacity - self.allocated_memory

  def __resource_round_up(self, x, base=4):
    return base * (int(math.ceil(x/base)))