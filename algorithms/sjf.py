from operator import attrgetter
from core.algorithm import Algorithm
from algorithms.common import load_balance_allocation


class ShortestJobFirst(Algorithm):
  def __call__(self, cluster, clock):
    jobs = cluster.jobs_in_waiting_queue
    
    # Sort jobs by duration
    jobs.sort(key=attrgetter('duration'))
    if(len(jobs) == 0):
      return None, []
    else:
      return load_balance_allocation(jobs[0], cluster)