import math
from operator import attrgetter
from core.algorithm import Algorithm
from algorithms.common import load_balance_allocation, backfill_plan


class UNICEP(Algorithm):
  """
    job submit: s_i,
    job duration: r_i,
    job size (nnodes): n_i, nnodes
    job scale: a_i, nnodes * duration
    job waiting time: w_i, clock - s_i
    
    piority function: f = -w_i/(log(n_i) * r_i), provides fast turnaround for small jobs
  """
  def __call__(self, cluster, clock, backfill):
    jobs = cluster.jobs_in_waiting_queue
    
    # Calculate the priority of each job using the WFP3 formula
    for job in jobs:
      job.priority = -((clock - job.submit)/(math.log(job.nnodes) * job.duration))
      
    # Sort jobs by priority
    jobs.sort(key=attrgetter('priority'))
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
    