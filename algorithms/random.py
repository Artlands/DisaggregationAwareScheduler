import math
import random
from operator import attrgetter
from core.algorithm import Algorithm


class Random(Algorithm):
  def __call__(self, cluster, clock):
    total_free_compute_nodes = cluster.total_free_compute_nodes
    total_memory_nodes = cluster.total_memory_nodes
    compute_node_memory_capacity = cluster.compute_node_memory_capacity
    memory_granularity = cluster.memory_granularity
    
    jobs = cluster.jobs_in_waiting_queue
    candidate_job = None
    compute_memory_node_tuples = [] #(compute_node, memory_node, remote_memory)

    jobs.sort(key=attrgetter('submit'))
    
    if total_free_compute_nodes and jobs:
      for job in jobs:
        # First check if the cluster has enough resources for the job
        if cluster.accommodate(job):
          random.shuffle(total_free_compute_nodes)
          candidate_nodes = total_free_compute_nodes[:job.nnodes]
          candidate_job = job
          
          # Do not need memory node
          if compute_node_memory_capacity >= job.memory:
            for c_node in candidate_nodes:
              compute_memory_node_tuples.append( (c_node, None, 0) )
            return candidate_job, compute_memory_node_tuples
          else:
            # Find remote memory in memory nodes randomly.
            job_remote_memory = job.memory - compute_node_memory_capacity
            job_remote_memory_round_up = self.resource_round_up(memory_granularity, job_remote_memory)
            memory_node_capacity_records = {} # Key: memory_node_id, Value: memory_node_free_memory
            
            # For each compute node, find a memory node for it
            for c_node in candidate_nodes:
              random.shuffle(total_memory_nodes)
              flag = False # Flag the availability of memory nodes
              for m_node in total_memory_nodes:
                if m_node.id in memory_node_capacity_records:
                  if memory_node_capacity_records[m_node.id] >= job_remote_memory_round_up:
                    memory_node_capacity_records[m_node.id] -= job_remote_memory_round_up
                    compute_memory_node_tuples.append( (c_node, m_node, job_remote_memory_round_up) )
                    flag = True
                    break
                else:
                  if m_node.free_memory >= job_remote_memory_round_up:
                    memory_node_capacity_records[m_node.id] = m_node.free_memory - job_remote_memory_round_up
                    compute_memory_node_tuples.append( (c_node, m_node, job_remote_memory_round_up) )
                    flag = True
                    break
              
              # Fail to find a memory node
              if flag == False:
                return None, []
              
          # all compute nodes should find memory resources from their corresponding memory nodes
          return candidate_job, compute_memory_node_tuples
        # Cannot accommodate the job
        return None, []
    # No jobs or no free compute nodes
    return None, []
        
  
  def resource_round_up(self, memory_granularity, x):
    return memory_granularity * (int(math.ceil(x/memory_granularity)))