from operator import attrgetter
from core.algorithm import Algorithm
from algorithms.common import load_balance_allocation, backfill_plan


class LeastAreaFirst(Algorithm):
  def __call__(self, cluster, clock, backfill):
    jobs = cluster.jobs_in_waiting_queue
    
    # Sort jobs by area (nnodes * duration)
    jobs.sort(key=attrgetter('area'))
    if(len(jobs) == 0):
      return None, []
    else:
      job, compute_memory_node_tuples = load_balance_allocation(jobs[0], cluster)
    
    if (job != None):
      return job, compute_memory_node_tuples
    else:
      if backfill == True:
        # Try to backfill the jobs in the waiting queue
        if(len(jobs) >= 2):
          job = backfill_plan(jobs[0], jobs[1:], cluster, clock)
          if(job != None):
            job, compute_memory_node_tuples = load_balance_allocation(job, cluster)
            if(job != None):
              return job, compute_memory_node_tuples
      return None, []
    