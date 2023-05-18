import math
from operator import attrgetter
from core.algorithm import Algorithm


class FirstComeFirstServe(Algorithm):
  def __call__(self, cluster, clock):
    total_free_compute_nodes = cluster.total_free_compute_nodes
    compute_node_memory_capacity = cluster.compute_node_memory_capacity
    memory_granularity = cluster.memory_granularity
    
    jobs = cluster.jobs_in_waiting_queue
    candidate_job = None
    compute_memory_node_tuples = [] #(compute_node, memory_node, remote_memory)

    jobs.sort(key=attrgetter('submit'))
    
    racks = cluster.racks
    racks.sort(key=attrgetter('idx'), reverse=False)
    
    if total_free_compute_nodes and jobs:
      for job in jobs:
        # First check if the cluster has enough resources for the job
        if cluster.accommodate(job):
          candidate_job = job
          candidate_nodes = []
          nnodes = job.nnodes
          
          for r in racks:
            if r.number_of_free_compute_nodes >= nnodes:
              # This rack has enough free compute nodes
              candidate_nodes = r.free_compute_nodes[:nnodes]
              break
            else:
              # This rack does not have enough free compute nodes for this job,
              # allocate all free compute nodes in this rack and continue to the next rack
              candidate_nodes = r.free_compute_nodes
              nnodes -= r.number_of_free_compute_nodes

          # Do not need memory node
          if compute_node_memory_capacity >= job.memory:
            for c_node in candidate_nodes:
              compute_memory_node_tuples.append( (c_node, None, 0) )
            return candidate_job, compute_memory_node_tuples
          else:
            # Find remote memory in order of racks
            job_remote_memory = job.memory - compute_node_memory_capacity
            job_remote_memory_round_up = self.resource_round_up(memory_granularity, job_remote_memory)
            memory_node_capacity_records = {} # Key: memory_node_id, Value: memory_node_free_memory
            
            # For each compute node, find a memory node for it
            for c_node in candidate_nodes:
              flag = False
              for r in racks:
                if r.free_remote_memory >= job_remote_memory_round_up:
                  # This rack has enough free remote memory
                  for m in r.memory_nodes:
                    if m.id in memory_node_capacity_records:
                      if memory_node_capacity_records[m.id] >= job_remote_memory_round_up:
                        memory_node_capacity_records[m.id] -= job_remote_memory_round_up
                        compute_memory_node_tuples.append( (c_node, m, job_remote_memory_round_up) )
                        flag = True
                        break
                    else:
                      if m.free_memory >= job_remote_memory_round_up:
                        memory_node_capacity_records[m.id] = m.free_memory - job_remote_memory_round_up
                        compute_memory_node_tuples.append( (c_node, m, job_remote_memory_round_up) )
                        flag = True
                        break
                  if flag == True:
                    break # break the for loop of racks
              
              # Fail to find the memory node in all racks
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