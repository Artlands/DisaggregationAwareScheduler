class Scheduler(object):
  def __init__(self, env, algorithm):
    self.env = env
    self.algorithm = algorithm
    self.simulation = None
    self.cluster = None
    self.destroyed = False

  def attach(self,simulation):
    self.simulation = simulation
    self.cluster = simulation.cluster

  def make_decision(self):
    while True:
      nodes, job = self.algorithm(self.cluster, self.env.now)
      if nodes is None or job is None:
        break
      else:
        job.start(nodes)

  def run(self):
    while not self.simulation.finished:
      self.make_decision()
      yield self.env.timeout(1)
    self.destroyed = True