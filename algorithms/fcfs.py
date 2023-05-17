import random
from operator import attrgetter
from core.algorithm import Algorithm


class FirstComeFirstServeAlgorithm(Algorithm):
  def __call__(self, cluster, clock):
    total_free_compute_nodes = cluster.total_free_compute_nodes
    jobs = cluster.jobs_in_waiting_queue
    candidate_job = None
    compute_memory_node_tuples = [] #(compute_node, memory_node, remote_memory)
    
    jobs.sort(key=attrgetter('submit'))
    if total_free_compute_nodes and jobs:
      for job in jobs:
        # First check if the cluster has enough resrouces for the job
        if cluster.accommodate(job):
          random.shuffle(total_free_compute_nodes)
          candidate_nodes = total_free_compute_nodes[:job.nnodes]
          candidate_job = job
          
          for c_node in candidate_nodes:
            compute_memory_node_tuples.append( (c_node, None, 0) )
          return candidate_job, compute_memory_node_tuples
        
        # Cannot accommodate the job
        return None, []
    
    # No jobs or no free compute nodes
    return None, []