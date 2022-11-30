from core.node import Node


class Rack(object):
  idx = 0
  def __init__(self):
    self.id = Rack.idx
    self.nodes = []
    self.cluster = None
    Rack.idx += 1
  
  def attach(self, cluster):
    self.cluster = cluster

  def add_nodes(self, node_configs):
    for node_config in node_configs:
      node = Node(node_config)
      self.nodes.append(node)
      node.attach(self, self.cluster)

  def accommodate(self, job):
    # Check node and memory requirement
    return self.free_nodes >= job.nnodes \
           and self.free_memory >= (job.memory * job.nnodes)

  @property
  def free_memory(self):
    free_memory = 0
    for node in self.nodes:
      free_memory += node.free_memory
    return free_memory

  @property
  def free_nodes(self):
    free_nodes = []
    for node in self.nodes:
      if not node.allocated:
        free_nodes.append(node)
    return free_nodes

  @property
  def busy_nodes(self):
    busy_nodes = []
    for node in self.nodes:
      if node.allocated:
        busy_nodes.append(node)
    return busy_nodes

    