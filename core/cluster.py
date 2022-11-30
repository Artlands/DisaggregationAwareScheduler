import operator
from core.rack import Rack


class Cluster(object):
  def __init__(self, nodes_per_rack = 256):
    self.racks = []
    self.jobs = []
    self.nodes_per_rack = nodes_per_rack
  
  def add_racks(self, node_configs):
    rack_node_configs = []
    for node_config in node_configs:
      rack_node_configs.append(node_config)
      if len(rack_node_configs) == self.nodes_per_rack:
        rack = Rack()
        self.racks.append(rack)
        rack.add_nodes(rack_node_configs)
        rack.attach(self)
        rack_node_configs = []
    if rack_node_configs:
      rack = Rack()
      self.racks.append(rack)
      rack.add_nodes(rack_node_configs)
      rack.attach(self)

  def add_job(self, job):
    self.jobs.append(job)

  def accommodate(self, job):
    return len(self.total_free_nodes) >= job.nnodes \
            and self.total_free_memory >= (job.memory * job.nnodes)

  @property
  def total_nodes(self):
    ls = []
    for rack in self.racks:
      ls.extend(rack.nodes)
    ls.sort(key=operator.attrgetter('id'))
    return ls

  @property
  def total_free_nodes(self):
    ls = []
    for node in self.total_nodes:
      if not node.allocated:
        ls.append(node)
    # Sort nodes by node id
    ls.sort(key=operator.attrgetter('id'))
    return ls

  @property
  def total_memory_capacity(self):
    return sum([node.memory_capacity for node in self.total_nodes])

  @property
  def total_free_memory(self):
    return sum([node.free_memory for node in self.total_nodes])

  @property
  def finished_jobs(self):
    ls = []
    for job in self.jobs:
      if job.finished:
        ls.append(job)
    ls.sort(key=operator.attrgetter('id'))
    return ls

  @property
  def unfinished_jobs(self):
    ls = []
    for job in self.jobs:
      if not job.finished:
        ls.append(job)
    ls.sort(key=operator.attrgetter('id'))
    return ls

  @property
  def running_jobs(self):
    running_jobs = []
    for node in self.total_nodes:
      if node.job and node.job not in running_jobs:
          running_jobs.append(node.job)
    return running_jobs

  @property
  def jobs_in_waiting_queue(self):
    ls = []
    for job in self.jobs:
      if not job.started:
        ls.append(job)
    ls.sort(key=operator.attrgetter('submit'))
    return ls

  @property
  def state(self):
    return {
      'arrived_jobs': len(self.jobs),
      'finished_jobs': len(self.finished_jobs),
      'running_jobs':  len(self.running_jobs),
      'jobs_in_waiting_queue': len(self.jobs_in_waiting_queue),
      'nodes_utilization': (len(self.total_nodes) - len(self.total_free_nodes))/(len(self.total_nodes)),
      'total_memory_utilization': (self.total_memory_capacity - self.total_free_memory)/self.total_memory_capacity
    }

  @property
  def jobs_summary(self):
    jobs_summary = {}
    for job in self.jobs:
      jobs_summary[job.id] = {
        'submit': int(job.submit),
        'start': int(job.started_timestamp),
        'end': int(job.finished_timestamp),
        'nnodes': int(job.nnodes),
        'memory': int(job.memory),
        'duration': int(job.duration),
      }
    return jobs_summary
