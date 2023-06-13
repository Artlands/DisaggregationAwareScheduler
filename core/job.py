class JobConfig(object):
  def __init__(self, jobid, submit, nnodes, max_node_memory, duration):
    self.id = jobid
    self.submit = submit
    self.nnodes = nnodes
    self.memory = max_node_memory
    self.duration = duration

class Job(object):
  idx = 0
  def __init__(self, env, job_config, raw_id, warmup_threshold):
    self.env = env
    self.cluster = None
    
    if raw_id:
      self.id = job_config.id
    else:
      self.id = Job.idx
    
    self.submit = job_config.submit
    self.nnodes = job_config.nnodes
    self.memory = job_config.memory

    self.duration = job_config.duration
    self.scale = self.nnodes * self.duration
    self.priority = 0
    self.slowdown = 0

    self.allocated_nodes = None
    self.allocated_memory_nodes = None
    self.process = None
    
    self.cross_rack_allocation_counts = 0
    self.cross_rack_allocation_capacity = 0

    self.started = False
    self.start = 0
    self.finished = False
    self.finish = 0
    self.failed = False
    
    self.warmup_threshold = warmup_threshold
    Job.idx += 1
    
  def attach(self, cluster):
    self.cluster = cluster
    
  def add_statistic(self, count, capacity):
    self.cross_rack_allocation_counts += count
    self.cross_rack_allocation_capacity += capacity

  def do_work(self):
    yield self.env.timeout(self.duration)
    self.finished = True
    self.finish = self.env.now
    if self.cluster.job_status == True:
      print(f'Job {self.id} finishs time: {self.env.now}')

    for node in self.allocated_nodes:
      node.stop_job(self)

    if self.allocated_memory_nodes:
      for remote_memory_record in self.allocated_memory_nodes:
        memory_node = remote_memory_record['memory_node']
        remote_memory = remote_memory_record['remote_memory']
        memory_node.deallocate_memory(self, remote_memory)
        
    # update cross-rack allocation statistics; only count the jobs after warmup_threshold
    if self.submit >= self.warmup_threshold:
      self.cluster.remove_cross_rack_allocation_statistic(self.cross_rack_allocation_counts, 
                                                          self.cross_rack_allocation_capacity)
        
  def fail(self):
    self.failed = True
    self.failt = self.env.now
    if self.cluster.job_status == True:
      print(f'Job {self.id} fails time: {self.env.now}')
    # Remote this job from the cluster job list
    self.cluster.remove_job(self)
    

  def run(self, nodes, memory_nodes):
    self.started = True
    self.start = self.env.now
    if self.cluster.job_status == True:
      print(f'Job {self.id} starts time: {self.start}')

    self.allocated_nodes = nodes
    for node in self.allocated_nodes:
      node.run_job(self)

    # Allocate remote memory on memory nodes
    if memory_nodes:
      self.allocated_memory_nodes = memory_nodes
      for remote_memory_record in self.allocated_memory_nodes:
        memory_node = remote_memory_record['memory_node']
        remote_memory = remote_memory_record['remote_memory']
        memory_node.allocate_memory(self, remote_memory)
    
    # update cross-rack allocation statistics; only count the jobs after warmup_threshold
    if self.submit >= self.warmup_threshold:
      self.cluster.add_cross_rack_allocation_statistic(self.cross_rack_allocation_counts, 
                                                      self.cross_rack_allocation_capacity)

    self.process = self.env.process(self.do_work())
