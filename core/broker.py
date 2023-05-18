import math
from core.job import Job


class Broker(object):
  job_cls = Job
  def __init__(self, env, jobs_configs, raw_id = True):
    print(f'Initializing job broker')
    self.env = env
    self.simulation = None
    self.cluster = None
    self.destroyed = False
    self.jobs_configs = jobs_configs
    self.raw_id = raw_id

  def attach(self, simulation):
    self.simulation = simulation
    self.cluster = simulation.cluster
    self.memory_granularity = self.cluster.memory_granularity
    
  def resource_round_up(self, x):
    return self.memory_granularity * (int(math.ceil(x/self.memory_granularity)))

  def run(self):
    for job_config in self.jobs_configs:
      assert job_config.submit >=self.env.now
      yield self.env.timeout(job_config.submit - self.env.now)
      job = Broker.job_cls(self.env, job_config, self.raw_id)
      job.attach(self.cluster)
      
      if self.cluster.job_status == True:
        print(f'Job {job.id} submits time: {self.env.now}, nnode: {job.nnodes}, memory: {job.memory}')
      
      # Check if job requests more nodes than the cluster has
      if job.nnodes > len(self.cluster.total_compute_nodes):
        self.cluster.add_failed_jobs(job, 'out-of-available-nodes')
      else:
        # Check if job requests more memories than the cluster has   
        if job.memory > self.cluster.compute_node_memory_capacity:
          if self.cluster.disaggregation:
            remote_memory = job.memory - self.cluster.compute_node_memory_capacity
            remote_memory_round_up = self.resource_round_up(remote_memory)
            memory_node_memory_capacity = self.cluster.memory_node_memory_capacity
            memory_units_supported_per_node = int(math.floor(memory_node_memory_capacity/remote_memory_round_up))
            memory_units_supported = memory_units_supported_per_node * (len(self.cluster.total_memory_nodes))
            if memory_units_supported >= job.nnodes:
              self.cluster.add_job(job)
            else:
              self.cluster.add_failed_jobs(job, 'out-of-memory')
          else:
              self.cluster.add_failed_jobs(job, 'out-of-memory')
        else:
          self.cluster.add_job(job)
      
    self.destroyed = True