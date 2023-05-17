import json


class Monitor(object):
  def __init__(self, simulation):
    self.simulation = simulation
    self.env = simulation.env
    self.cluster_state_file = simulation.cluster_state_file
    self.jobs_summary_file = simulation.jobs_summary_file
    self.cluster_state = []
    self.jobs_summary = {}

  def run(self):
    while not self.simulation.finished:
      state = {
        'timestamp': self.env.now,
        'cluster_state': self.simulation.cluster.state
      }
      self.cluster_state.append(state)
      yield self.env.timeout(1)
    
    state = {
      'timestamp': self.env.now,
      'cluster_state': self.simulation.cluster.state
    }
    self.cluster_state.append(state)
    if self.cluster_state_file:
      self.write_cluster_state()

    if self.jobs_summary_file:
      self.jobs_summary = self.simulation.cluster.jobs_summary
      self.write_jobs_summary()

  def write_cluster_state(self):
    print('Writing cluster state to file')
    with open(self.cluster_state_file, 'w') as f:
      json.dump(self.cluster_state, f, indent=4)
  
  def write_jobs_summary(self):
    print('Writing jobs summary to file')
    with open(self.jobs_summary_file, 'w') as f:
      json.dump(self.jobs_summary, f, indent=4)