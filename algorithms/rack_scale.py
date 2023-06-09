from operator import attrgetter
from core.algorithm import Algorithm
from algorithms.common import rack_scale_allocation, backfill_plan


class RackScale(Algorithm):
  def __call__(self, cluster, clock, backfill):
    jobs = cluster.jobs_in_waiting_queue

    jobs.sort(key=attrgetter('submit'))
    if(len(jobs) == 0):
      return None, []
    else:
      job, compute_memory_node_tuples = rack_scale_allocation(jobs[0], cluster)
    
    if (job != None):
      return job, compute_memory_node_tuples
    else:
      if backfill == True:
        # Try to backfill the jobs in the waiting queue
        if(len(jobs) >= 2):
          job = backfill_plan(jobs[0], jobs[1:], cluster, clock)
          if(job != None):
            job, compute_memory_node_tuples = rack_scale_allocation(job, cluster)
            if(job != None):
              return job, compute_memory_node_tuples
      return None, []