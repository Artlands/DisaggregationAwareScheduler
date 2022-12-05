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

  def add_nodes(self, node_configs, memory_granularity):
    for node_config in node_configs:
      if node_config.ntype == 'compute':
        node = ComputeNode(node_config, memory_granularity)
        # print(f'Add node: {node.id}')
        self.compute_nodes.append(node)
      elif node_config.ntype == 'memory':
        node = MemoryNode(node_config, memory_granularity)
        self.memory_nodes.append(node)
      else:
        pass
      node.attach(self, self.cluster)

  def accommodate(self, job):
    return len(self.free_compute_nodes) >= job.nnodes
  
  @property
  def number_of_free_compute_nodes(self):
    return len(self.free_compute_nodes)

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
