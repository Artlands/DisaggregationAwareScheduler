class JobConfig(object):
  def __init__(self, submit, nnodes, max_node_memory, duration):
    self.submit = submit
    self.nnodes = nnodes
    self.memory = max_node_memory
    self.duration = duration


class Job(object):
  idx = 0
  def __init__(self, env, job_config):
    self.env = env
    self.id = Job.idx
    self.submit = job_config.submit
    self.nnodes = job_config.nnodes
    self.memory = job_config.memory
    self.duration = job_config.duration

    self.allocated_nodes = None
    self.process = None

    self.started = False
    self.started_timestamp = None
    self.allocated_nodes = None
    self.finished = False
    self.finished_timestamp = None
    Job.idx += 1

  def do_work(self):
    yield self.env.timeout(self.duration)
    self.finished = True
    self.finished_timestamp = self.env.now
    print(f'Job {self.id} finish time: {self.env.now}')
    for node in self.allocated_nodes:
      node.stop_job(self)

  def start(self, nodes):
    self.started = True
    self.started_timestamp = self.env.now
    print(f'Job {self.id} start time: {self.started_timestamp}')
    self.allocated_nodes = nodes
    for node in self.allocated_nodes:
      node.run_job(self)
    self.process = self.env.process(self.do_work())
