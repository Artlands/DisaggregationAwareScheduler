from core.monitor import Monitor


class Simulation(object):
  def __init__(self, env, cluster, job_broker, scheduler, cluster_state_file=None, jobs_summary_file=None):
    self.env = env
    self.cluster = cluster
    self.job_broker = job_broker
    self.scheduler = scheduler
    self.cluster_state_file = cluster_state_file
    self.jobs_summary_file = jobs_summary_file
    if cluster_state_file is not None or jobs_summary_file is not None:
      self.monitor = Monitor(self)

    self.job_broker.attach(self)
    self.scheduler.attach(self)
  
  def run(self):
    if self.cluster_state_file is not None or self.jobs_summary_file is not None:
      self.env.process(self.monitor.run())
    self.env.process(self.job_broker.run())
    self.env.process(self.scheduler.run())

  @property
  def finished(self):
    return self.job_broker.destroyed and len(self.cluster.unfinished_jobs) == 0