from core.node import ComputeNode, MemoryNode


class Rack(object):
  idx = 0
  def __init__(self):
    self.id = Rack.idx
    self.compute_nodes = []
    self.memory_nodes = []
    self.cluster = None
    Rack.idx += 1
  
  def attach(self, cluster):
    self.cluster = cluster

  def add_nodes(self, node_configs):
    for node_config in node_configs:
      if node_config.ntype == 'compute':
        node = ComputeNode(node_config)
        self.compute_nodes.append(node)
      elif node_config.ntype == 'memory':
        node = MemoryNode(node_config)
        self.memory_nodes.append(node)
      else:
        pass
      node.attach(self, self.cluster)

  def accommodate(self, job, disaggregation=False):
    # Check the memory requirement, if exceeds the node memory capacity,
    # mark the job failed.
    if not disaggregation:
      if job.memory > self.compute_node_memory_capacity:
        job.failed = True
        job.started = True
        job.finished = True
        return False
    return len(self.free_compute_nodes) >= job.nnodes

  @property
  def compute_node_memory_capacity(self):
    return self.compute_nodes[0].memory_capacity

  @property
  def free_local_memory(self):
    free_local_memory = 0
    for node in self.compute_nodes:
      free_local_memory += node.free_memory
    return free_local_memory

  @property
  def free_remote_memory(self):
    free_remote_memory = 0
    for node in self.memory_nodes:
      free_remote_memory += node.free_memory
    return free_remote_memory

  @property
  def free_compute_nodes(self):
    free_compute_nodes = []
    for node in self.compute_nodes:
      if not node.allocated:
        free_compute_nodes.append(node)
    return free_compute_nodes

  @property
  def busy_compute_nodes(self):
    busy_compute_nodes = []
    for node in self.compute_nodes:
      if node.allocated:
        busy_compute_nodes.append(node)
    return busy_compute_nodes
