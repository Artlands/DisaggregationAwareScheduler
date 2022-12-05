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
      job, nodes, memory_nodes = self.algorithm(self.cluster, self.env.now)
      # print(f'Job {job.id}, nodes {[node.id for node in nodes]}')
      if (len(nodes) == 0) or (job == None):
        break
      else:
        job.start(nodes, memory_nodes)

  def run(self):
    while not self.simulation.finished:
      self.make_decision()
      yield self.env.timeout(1)
    self.destroyed = True