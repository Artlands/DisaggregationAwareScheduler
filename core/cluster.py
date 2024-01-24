import operator
from core.node import NodeConfig
from core.rack import Rack


class Cluster(object):
  def __init__(self, env, cluster_config):
    print(f'Initializing cluster')
    self.env = env
    self.racks = []
    self.jobs = []
    self.failed_jobs = []
    self.node_status = cluster_config.node_status
    self.job_status = cluster_config.job_status
    self.disaggregation = cluster_config.disaggregation
    self.total_racks = cluster_config.total_racks
    self.compute_nodes_per_rack = cluster_config.compute_nodes_per_rack
    self.memory_nodes_per_rack = cluster_config.memory_nodes_per_rack
    self.compute_node_memory_capacity = cluster_config.compute_node_memory_capacity
    self.memory_node_memory_capacity = cluster_config.memory_node_memory_capacity
    self.memory_granularity = cluster_config.memory_granularity
    self.cross_rack_allocation_counts = 0
    self.cross_rack_allocation_capacity = 0
    self.time_series = cluster_config.time_series
    
    for _ in range(self.total_racks):
      node_configs = []
      for c in range(self.compute_nodes_per_rack):
        node_configs.append(NodeConfig(self.compute_node_memory_capacity, 'compute'))
      for m in range(self.memory_nodes_per_rack):
        node_configs.append(NodeConfig(self.memory_node_memory_capacity, 'memory'))

      rack = Rack()
      self.add_rack(rack)
      rack.add_nodes(node_configs, self.memory_granularity)
  
  def add_rack(self, rack):
    self.racks.append(rack)
    rack.attach(self)

  def add_job(self, job):
    self.jobs.append(job)
    
  def remove_job(self, job):
    if job in self.jobs:
      self.jobs.remove(job)
      
  def add_cross_rack_allocation_statistic(self, count, capacity):
    self.cross_rack_allocation_counts += count
    self.cross_rack_allocation_capacity += capacity
    
  def remove_cross_rack_allocation_statistic(self, count, capacity):
    self.cross_rack_allocation_counts -= count
    self.cross_rack_allocation_capacity -= capacity
    
  def add_failed_jobs(self, job, reason):
    self.failed_jobs.append({
      'job': job,
      'reason': reason
    })

  def find_rack(self, rack_id):
    for rack in self.racks:
      if rack.id == rack_id:
        return rack
    return -1
  
  def find_job(self, job_id):
    for job in self.jobs:
      if job.id == job_id:
        return job
    return -1
  
  def find_computer_node(self, node_id):
    for node in self.total_compute_nodes:
      if node.id == node_id:
        return node
    return -1
  
  def find_memory_node(self, node_id):
    for node in self.total_memory_nodes:
      if node.id == node_id:
        return node
    return -1

  def accommodate(self, job):
    return len(self.total_free_compute_nodes) >= job.nnodes

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
    # print(f'Total compute nodes: {len(self.total_compute_nodes)}')
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
      if not job.finished and not job.failed:
        ls.append(job)
    ls.sort(key=operator.attrgetter('id'))
    return ls

  @property
  def running_jobs(self):
    running_jobs = []
    for node in self.total_compute_nodes:
      if node.job and node.job not in running_jobs:
          running_jobs.append(node.job)
    return running_jobs

  @property
  def jobs_in_waiting_queue(self):
    ls = []
    for job in self.jobs:
      if not job.started and not job.failed:
        ls.append(job)
    ls.sort(key=operator.attrgetter('submit'))
    return ls
  
  @property
  def remote_memory_in_use(self):
    total_rmt_m_in_use = 0
    running_jobs = self.running_jobs
    for job in running_jobs:
      if self.time_series:
        m_use_idx = self.env.now - job.start
        m_use = job.memory[m_use_idx]
      else:
        m_use = job.max_memory
      if m_use > self.compute_node_memory_capacity:
        rmt_m_use = m_use - self.compute_node_memory_capacity
        total_rmt_m_in_use += rmt_m_use*job.nnodes
        
    return total_rmt_m_in_use

  @property
  def state(self):
    if self.total_remote_memory_capacity == 0:
      total_remote_memory_utilization = 0
    else: 
      total_remote_memory_utilization = (self.total_remote_memory_capacity \
        - self.total_remote_free_memory)/self.total_remote_memory_capacity
        
    return {
      'arrived_jobs': len(self.jobs),
      'finished_jobs': len(self.finished_jobs),
      'failed_jobs': len(self.failed_jobs),
      'running_jobs':  len(self.running_jobs),
      'jobs_in_waiting_queue': len(self.jobs_in_waiting_queue),
      'jobs_average_waiting_time_in_queue': self.jobs_average_waiting_time_in_queue,
      'cross_rack_allocation_counts': self.cross_rack_allocation_counts,
      'cross_rack_allocation_capacity': self.cross_rack_allocation_capacity,
      'compute_nodes_utilization': (len(self.total_compute_nodes) - len(self.total_free_compute_nodes))/(len(self.total_compute_nodes)),
      'total_remote_memory_in_use': int(self.remote_memory_in_use),
      'total_local_memory_utilization': (self.total_local_memory_capacity - self.total_local_free_memory)/self.total_local_memory_capacity,
      'total_remote_memory_utilization': total_remote_memory_utilization
    }
  
  @property
  def jobs_average_waiting_time_in_queue(self):
    total_waiting_time = 0
    for job in self.jobs_in_waiting_queue:
      total_waiting_time += self.env.now - job.submit
    if len(self.jobs_in_waiting_queue) == 0:
      return 0
    return total_waiting_time/len(self.jobs_in_waiting_queue)

  @property
  def jobs_summary(self):
    jobs_summary = {}
    for job in self.jobs:
      job_id_str = str(job.id)
      jobs_summary[job_id_str] = {
        'submit': int(job.submit),
        'start': int(job.start),
        'finish': int(job.finish),
        'nnodes': int(job.nnodes),
        'max_memory': int(job.max_memory),
        'duration': int(job.duration),
        'slowdown': job.slowdown,
        'failed': False,
        'reason': None,
        'stall': job.stall,
        'total_memory_scale': int(job.total_memory_scale),
        'remote_memory_scale': int(job.remote_memory_scale),
        'cross_rack_allocation_counts': job.cross_rack_allocation_counts,
        'cross_rack_allocation_capacity': job.cross_rack_allocation_capacity
      }
    for record in self.failed_jobs:
      job = record['job']
      reason = record['reason']
      job_id_str = str(job.id)
      jobs_summary[job_id_str] = {
        'submit': int(job.submit),
        'start': 0,
        'finish': 0,
        'nnodes': int(job.nnodes),
        'max_memory': int(job.max_memory),
        'duration': 0,
        'slowdown': 1,
        'failed': True,
        'reason': reason,
        'stall': job.stall,
        'total_memory_scale': int(job.total_memory_scale),
        'remote_memory_scale': int(job.remote_memory_scale),
        'cross_rack_allocation_counts': job.cross_rack_allocation_counts,
        'cross_rack_allocation_capacity': job.cross_rack_allocation_capacity
      }
    return jobs_summary
