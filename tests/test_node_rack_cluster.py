from core.cluster import Cluster
from core.rack import Rack
from core.node import NodeConfig, ComputeNode, MemoryNode


def test_compute_node():
  compute_node_config = NodeConfig(256, 'compute')
  compute_node = ComputeNode(compute_node_config)
  assert compute_node.id == 0
  assert compute_node.ntype == 'compute'
  assert compute_node.memory_capacity == 256


def test_memory_node():
  memory_node_config = NodeConfig(2048, 'memory')
  memory_node = MemoryNode(memory_node_config)
  assert memory_node.id == 1
  assert memory_node.ntype == 'memory'
  assert memory_node.memory_capacity == 2048


def test_rack():
  compute_node_config = NodeConfig(256, 'compute')
  memory_node_config = NodeConfig(2048, 'memory')
  node_configs = [compute_node_config for _ in range(10)] + [memory_node_config for _ in range(2)]
  rack = Rack()
  rack.add_nodes(node_configs)
  assert len(rack.compute_nodes) == 10
  assert len(rack.memory_nodes) == 2


def test_cluster():
  cluster = Cluster()
  nracks = 6
  racks = []
  for n in range(nracks):
    compute_node_config = NodeConfig(256, 'compute')
    memory_node_config = NodeConfig(2048, 'memory')
    node_configs = [compute_node_config for _ in range(10)] + [memory_node_config for _ in range(2)]
    rack = Rack()
    rack.add_nodes(node_configs)
    racks.append(rack)
  cluster.add_racks(racks)
  assert len(cluster.total_compute_nodes) == 60
  assert len(cluster.total_memory_nodes) == 12
  assert cluster.racks[0].id == 1