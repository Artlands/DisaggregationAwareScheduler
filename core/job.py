class JobConfig(object):
  def __init__(self, jobid, submit, nnodes, max_memory, memory, duration):
    self.id = jobid
    self.submit = submit
    self.nnodes = nnodes
    self.memory = memory
    self.max_memory = max_memory
    self.duration = duration

class Job(object):
  idx = 0
  def __init__(self, env, job_config, raw_id):
    self.env = env
    self.cluster = None
    
    if raw_id:
      self.id = job_config.id
    else:
      self.id = Job.idx
    
    self.submit = job_config.submit
    self.nnodes = job_config.nnodes
    self.memory = job_config.memory
    self.max_memory = job_config.max_memory

    self.duration = job_config.duration
    self.scale = self.nnodes * self.duration
    self.priority = 0
    self.slowdown = 0

    self.allocated_nodes = None
    self.allocated_memory_nodes = None
    self.process = None
    
    self.remote_memory_ratio = 0
    self.total_memory_scale = sum(self.memory)  # the amount of total memory allocated to this job * duration
    self.remote_memory_scale = 0 # the amount of remote memory allocated to this job * duration of remote memory access
    
    self.cross_rack_allocation_counts = 0
    self.cross_rack_allocation_capacity = 0

    self.started = False
    self.start = 0
    self.finished = False
    self.finish = 0
    self.failed = False
    
    self.stall = 0
    Job.idx += 1
    
  def attach(self, cluster):
    self.cluster = cluster
    # After attaching to the cluster, we can know the job's remote memory scale
    compute_node_memory_capacity = self.cluster.compute_node_memory_capacity
    remote_memory_records = [(m-compute_node_memory_capacity) for m in self.memory if m > compute_node_memory_capacity]
    self.remote_memory_scale = sum(remote_memory_records)
    if compute_node_memory_capacity < self.max_memory:
      self.remote_memory_ratio = self.max_memory/compute_node_memory_capacity
    
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
        
    # update cross-rack allocation statistics
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
    
    # update cross-rack allocation statistics
    self.cluster.add_cross_rack_allocation_statistic(self.cross_rack_allocation_counts, 
                                                      self.cross_rack_allocation_capacity)

    self.process = self.env.process(self.do_work())
