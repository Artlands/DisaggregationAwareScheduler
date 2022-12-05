class JobConfig(object):
  def __init__(self, jobid, submit, nnodes, max_node_memory, duration):
    self.id = jobid
    self.submit = submit
    self.nnodes = nnodes
    self.memory = max_node_memory
    self.duration = duration

class Job(object):
  idx = 0
  def __init__(self, env, job_config, raw_id):
    self.env = env
    
    if raw_id:
      self.id = job_config.id
    else:
      self.id = Job.idx
    
    self.submit = job_config.submit
    self.nnodes = job_config.nnodes
    self.memory = job_config.memory
    # self.local_memory = min(self.memory, compute_node_memory_capacity)
    # self.remote_memory = max(0, self.memory - compute_node_memory_capacity)
    self.duration = job_config.duration

    self.allocated_nodes = None
    self.allocated_memory_nodes = None
    self.process = None

    self.started = False
    self.started_timestamp = None
    self.finished = False
    self.finished_timestamp = None
    Job.idx += 1

  def do_work(self):
    yield self.env.timeout(self.duration)
    self.finished = True
    self.finished_timestamp = self.env.now
    print(f'Job {self.id} finishes @: {self.env.now}')

    for node in self.allocated_nodes:
      node.stop_job(self)

    if self.allocated_memory_nodes:
      for remote_memory_record in self.allocated_memory_nodes:
        memory_node = remote_memory_record['memory_node']
        remote_memory = remote_memory_record['remote_memory']
        memory_node.deallocate_memory(self, remote_memory)

  def start(self, nodes, memory_nodes):
    self.started = True
    self.started_timestamp = self.env.now
    print(f'Job {self.id} starts @: {self.started_timestamp}')

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

    self.process = self.env.process(self.do_work())
