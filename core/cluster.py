import operator
from core.rack import Rack


class Cluster(object):
  def __init__(self):
    self.racks = []
    self.jobs = []
  
  def add_racks(self, racks):
    for rack in racks:
      self.racks.append(rack)
      rack.attach(self)

  def add_job(self, job):
    self.jobs.append(job)

  def accommodate(self, job, disaggregation=False):
    # Check the memory requirement, if exceeds the node memory capacity,
    # mark the job failed.
    if not disaggregation:
      if job.memory > self.compute_node_memory_capacity:
        job.failed = True
        job.started = True
        job.finished = True
        return False
    return len(self.total_free_compute_nodes) >= job.nnodes

  @property
  def compute_node_memory_capacity(self):
    return self.racks[0].compute_nodes[0].memory_capacity

  @property
  def total_compute_nodes(self):
    total_compute_nodes = []
    for rack in self.racks:
      total_compute_nodes.extend(rack.compute_nodes)
    total_compute_nodes.sort(key=operator.attrgetter('id'))
    return total_compute_nodes

  @property
  def total_memory_nodes(self):
    total_memory_nodes = []
    for rack in self.racks:
      total_memory_nodes.extend(rack.memory_nodes)
    total_memory_nodes.sort(key=operator.attrgetter('id'))
    return total_memory_nodes

  @property
  def total_free_compute_nodes(self):
    total_free_compute_nodes = []
    for node in self.total_compute_nodes:
      if not node.allocated:
        total_free_compute_nodes.append(node)
    # Sort nodes by node id
    total_free_compute_nodes.sort(key=operator.attrgetter('id'))
    return total_free_compute_nodes

  @property
  def total_local_memory_capacity(self):
    return sum([node.memory_capacity for node in self.total_compute_nodes])

  @property
  def total_remote_memory_capacity(self):
    return sum([node.memory_capacity for node in self.total_memory_nodes])

  @property
  def total_local_free_memory(self):
    return sum([node.free_memory for node in self.total_compute_nodes])

  @property
  def total_remote_free_memory(self):
    return sum([node.free_memory for node in self.total_memory_nodes])

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
  def failed_jobs(self):
    ls = []
    for job in self.jobs:
      if job.failed:
        ls.append(job)
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
      'failed_jobs': len(self.failed_jobs),
      'running_jobs':  len(self.running_jobs),
      'jobs_in_waiting_queue': len(self.jobs_in_waiting_queue),
      'compute_nodes_utilization': (len(self.total_compute_nodes) - len(self.total_free_compute_nodes))/(len(self.total_compute_nodes)),
      'total_local_memory_utilization': (self.total_local_memory_capacity - self.total_local_free_memory)/self.total_local_memory_capacity,
      'total_remote_memory_utilization': (self.total_remote_memory_capacity - self.total_remote_free_memory)/self.total_remote_memory_capacity
    }

  @property
  def jobs_summary(self):
    jobs_summary = {}
    for job in self.jobs:
      if not job.failed:
        jobs_summary[job.id] = {
          'submit': int(job.submit),
          'start': int(job.started_timestamp),
          'end': int(job.finished_timestamp),
          'nnodes': int(job.nnodes),
          'memory': int(job.memory),
          'duration': int(job.duration),
          'failed': job.failed
        }
      else:
        jobs_summary[job.id] = {
          'submit': int(job.submit),
          'start': 0,
          'end': 0,
          'nnodes': int(job.nnodes),
          'memory': int(job.memory),
          'duration': 0,
          'failed': job.failed
        }
    return jobs_summary
