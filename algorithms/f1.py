import math
from operator import attrgetter
from core.algorithm import Algorithm
from algorithms.common import load_balance_allocation, backfill_plan
from algorithms.common import rack_scale_allocation, system_scale_allocation


class F1(Algorithm):
  """
    job submit: s_i,
    job duration: r_i,
    job size (nnodes): n_i, nnodes
    job scale: a_i, nnodes * duration
    job waiting time: w_i, clock - s_i
    
    piority function: f = log10(r_i) * n_i + 870 * log10(s_i)
  """
  def __call__(self, cluster, clock, backfill, disaggregation, rack_scale):
    if disaggregation:
      if rack_scale:
        allocation_func = rack_scale_allocation
      else:
        allocation_func = system_scale_allocation
    else:
      allocation_func = load_balance_allocation
      
    jobs = cluster.jobs_in_waiting_queue
    
    # Calculate the priority of each job using the F1 formula
    for job in jobs:
      job.priority = math.log10(job.duration) * job.nnodes + 870 * math.log10(job.submit+1)
      
    # Sort jobs by priority
    jobs.sort(key=attrgetter('priority'))
    if(len(jobs) == 0):
      return None, []
    else:
      job, compute_memory_node_tuples = allocation_func(jobs[0], cluster)
    
    if (job != None):
      return job, compute_memory_node_tuples
    else:
      if backfill == True:
        # Try to backfill the jobs in the waiting queue
        if(len(jobs) >= 2):
          job = backfill_plan(jobs[0], jobs[1:], cluster, clock)
          if(job != None):
            job, compute_memory_node_tuples = allocation_func(job, cluster)
            if(job != None):
              return job, compute_memory_node_tuples
      return None, []
    