from core.job import Job


class Broker(object):
  job_cls = Job
  def __init__(self, env, jobs_configs):
    self.env = env
    self.simulation = None
    self.cluster = None
    self.destroyed = False
    self.jobs_configs = jobs_configs

  def attach(self, simulation):
    self.simulation = simulation
    self.cluster = simulation.cluster

  def run(self):
    for job_config in self.jobs_configs:
      assert job_config.submit >=self.env.now
      yield self.env.timeout(job_config.submit - self.env.now)
      job = Broker.job_cls(self.env, job_config)
      print(f'Job {job.id} submit time: {self.env.now}, nnode: {job.nnodes}, memory: {job.memory}')
      self.cluster.add_job(job)
    self.destroyed = True