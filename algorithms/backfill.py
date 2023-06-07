from operator import attrgetter
from core.algorithm import Algorithm
from algorithms.common import load_balance_allocation


class Backfill(Algorithm):
  def __call__(self, cluster, clock):
    jobs = cluster.jobs_in_waiting_queue

    # Sort jobs by priority
    jobs.sort(key=attrgetter('priority'))
    if(len(jobs) == 0):
      return None, []
    else:
      job, compute_memory_node_tuples = load_balance_allocation(jobs[0], cluster)
    
    if (job != None):
      return job, compute_memory_node_tuples
    else:
      # Try to find small jobs that may be able to run
      jobs.sort(key=attrgetter('nnodes'))
      for job in jobs:
        job, compute_memory_node_tuples = load_balance_allocation(job, cluster)
        if (job != None):
          return job, compute_memory_node_tuples
      return None, []
        
    