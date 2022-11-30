from core.node import NodeConfig, Node
from core.cluster import Cluster

def test_node():
  node_config = NodeConfig(512)
  node = Node(node_config)
  assert node.id == 0
  assert node.memory_capacity == 512


def test_cluster_1():
  node_configs = [NodeConfig(512) for x in range(30)]
  cluster = Cluster(nodes_per_rack=6)
  cluster.add_racks(node_configs)
  assert len(cluster.racks) == 5

  last_rack = cluster.racks[-1]
  assert len(last_rack.nodes) == 6

  last_node = last_rack.nodes[-1]
  assert last_node.id == 30

def test_cluster_2():
  node_configs = [NodeConfig(512) for x in range(30)]
  cluster = Cluster(nodes_per_rack=7)
  cluster.add_racks(node_configs)
  assert len(cluster.racks) == 5

  last_rack = cluster.racks[-1]
  assert len(last_rack.nodes) == 2
  

def test_cluster_3():
  node_configs = [NodeConfig(512) for x in range(30)]
  cluster = Cluster(nodes_per_rack=7)
  cluster.add_racks(node_configs)
  last_rack = cluster.racks[-1]
  total_free_nodes = last_rack.free_nodes
  assert len(total_free_nodes) == 2

  free_memory = last_rack.free_memory
  assert free_memory == 2 * 512


def test_cluster_4():
  node_configs = [NodeConfig(16) for x in range(5)]
  cluster = Cluster(nodes_per_rack=7)
  cluster.add_racks(node_configs)
  total_free_nodes = cluster.total_free_nodes
  assert len(total_free_nodes) == 5

  total_free_memory = cluster.total_free_memory
  assert total_free_memory == 16 * 5