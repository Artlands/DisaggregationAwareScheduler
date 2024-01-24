from operator import attrgetter
from core.algorithm import Algorithm
from algorithms.common import backfill_plan


class WFP3(Algorithm):
  """
    job submit: s_i,
    job duration: r_i,
    job size (nnodes): n_i, nnodes
    job scale: a_i, nnodes * duration
    job waiting time: w_i, clock - s_i
    
    Weighted Fairness Priority
    piority function: f = -(w_i/r_i)^3 * n_i, favors old/short jobs more, 
    avoiding large job starvation
  """
  def __call__(self, cluster, clock, backfill, allocation_func):
    # Get jobs in the waiting queue
    jobs = cluster.jobs_in_waiting_queue
          
    # Calculate the priority of each job using the WFP3 formula
    for job in jobs:
      job.priority = ((clock - job.submit)/job.duration)**3 * job.nnodes
      
    # Sort jobs by priority
    jobs.sort(key=attrgetter('priority'), reverse=True)
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
    