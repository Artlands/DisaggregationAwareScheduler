import random
from core.algorithm import Algorithm


class RandomAlgorithm(Algorithm):
  def __call__(self, cluster, clock):
    total_free_nodes = cluster.total_free_nodes
    jobs = cluster.jobs_in_waiting_queue
    candidate_job = None
    candidate_nodes = None

    if total_free_nodes and jobs:
      for job in jobs:
        # First check if the cluster has enough resrouces for the job
        if cluster.accommodate(job):
          random.shuffle(total_free_nodes)
          candidate_nodes = total_free_nodes[:job.nnodes]
          candidate_job = job
          break

    return candidate_nodes, candidate_job