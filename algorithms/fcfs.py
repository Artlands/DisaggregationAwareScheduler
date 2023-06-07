from operator import attrgetter
from core.algorithm import Algorithm
from algorithms.common import load_balance_allocation


class FirstComeFirstServe(Algorithm):
  def __call__(self, cluster, clock):
    jobs = cluster.jobs_in_waiting_queue

    # Sort jobs by submit time
    jobs.sort(key=attrgetter('submit'))
    if(len(jobs) == 0):
      return None, []
    else:
      return load_balance_allocation(jobs[0], cluster)
    