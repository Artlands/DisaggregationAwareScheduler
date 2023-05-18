import math
import random
from operator import attrgetter
from core.algorithm import Algorithm


class RackAware(Algorithm):
  def __call__(self, cluster, clock):
    total_free_compute_nodes = cluster.total_free_compute_nodes
    compute_node_memory_capacity = cluster.compute_node_memory_capacity
    memory_granularity = cluster.memory_granularity
    
    jobs = cluster.jobs_in_waiting_queue
    candidate_job = None
    compute_memory_node_tuples = [] #(compute_node, memory_node, remote_memory)

    jobs.sort(key=attrgetter('submit'))
    
    racks = cluster.racks
    racks.sort(key=attrgetter('number_of_free_compute_nodes'), reverse=True)
    top_rack = racks[0]
    
    if total_free_compute_nodes and jobs:
      for job in jobs:
        # First check if the cluster has enough resources for the job
        if cluster.accommodate(job):
          candidate_job = job
          candidate_nodes = []
          
          # Do not consider rack-awarenes, randomly select candidate compute nodes
          if compute_node_memory_capacity >=job.memory:
            random.shuffle(total_free_compute_nodes)
            candidate_nodes = total_free_compute_nodes[:job.nnodes]
          else:
            number_of_remaining_nodes = job.nnodes
            # Racks sorted by distance to the current top rack
            sort_rack_ids = self.sort_rack_ids(top_rack.id, len(racks))
            for rack_id in sort_rack_ids:
              select_rack = cluster.find_rack(rack_id)
              compute_nodes = select_rack.free_compute_nodes
              compute_nodes.sort(key=attrgetter('id'))
              # Allocate compute nodes in the current rack as much as possible.
              number_of_allocated_nodes = min(len(compute_nodes), number_of_remaining_nodes)
              # Update candidate lists
              if number_of_remaining_nodes > 0:
                candidate_nodes.extend(compute_nodes[:number_of_allocated_nodes])
                number_of_remaining_nodes -= number_of_allocated_nodes
                
              if number_of_remaining_nodes == 0:
                break
          
          # Should always be true since we've already checked the cluster nodes capacity and the job allocate enough free nodes.
          # assert number_of_remaining_nodes == 0
          
          # Do not need memory node
          if compute_node_memory_capacity >= job.memory:
            for c_node in candidate_nodes:
              compute_memory_node_tuples.append( (c_node, None, 0) )
            return candidate_job, compute_memory_node_tuples
          else:
            # Find remote memory in memory nodes
            job_remote_memory = job.memory - compute_node_memory_capacity
            job_remote_memory_round_up = self.resource_round_up(memory_granularity, job_remote_memory)
            memory_node_capacity_records = {} # Key: memory_node_id, Value: memory_node_free_memory
            
            # For each compute node, find a memory node for it
            for c_node in candidate_nodes:
              c_node_rack = c_node.rack
              m_nodes = c_node_rack.memory_nodes
              flag = False # Flag the availability of memory nodes
              for m_node in m_nodes:
                if m_node.id in memory_node_capacity_records:
                  if memory_node_capacity_records[m_node.id] >= job_remote_memory_round_up:
                    memory_node_capacity_records[m_node.id] -= job_remote_memory_round_up
                    compute_memory_node_tuples.append( (c_node, m_node, job_remote_memory_round_up) )
                    flag = True
                    break # break for m_nodes loop
                else:
                  if m_node.free_memory >= job_remote_memory_round_up:
                    memory_node_capacity_records[m_node.id] = m_node.free_memory - job_remote_memory_round_up
                    compute_memory_node_tuples.append( (c_node, m_node, job_remote_memory_round_up) )
                    flag = True
                    break # break for m_nodes loop
              
              # Fail to find the memory node in the same rack, try other racks
              if flag == False:
                sort_rack_ids = self.sort_rack_ids(c_node_rack.id, len(racks))
                for rack_id in sort_rack_ids[1:]:
                  select_rack = cluster.find_rack(rack_id)
                  m_nodes = select_rack.memory_nodes
                  for m_node in m_nodes:
                    if m_node.id in memory_node_capacity_records:
                      if memory_node_capacity_records[m_node.id] >= job_remote_memory_round_up:
                        memory_node_capacity_records[m_node.id] -= job_remote_memory_round_up
                        compute_memory_node_tuples.append( (c_node, m_node, job_remote_memory_round_up) )
                        flag = True
                        break # break for m_nodes loop
                    else:
                      if m_node.free_memory >= job_remote_memory_round_up:
                        memory_node_capacity_records[m_node.id] = m_node.free_memory - job_remote_memory_round_up
                        compute_memory_node_tuples.append( (c_node, m_node, job_remote_memory_round_up) )
                        flag = True
                        break # break for m_nodes loop
                  if flag == True:
                    break # break for racks loop
                
              # Fail to find the memory node in all racks
              if flag == False:
                return None, []
              
          # all compute nodes should find their memory nodes
          return candidate_job, compute_memory_node_tuples
        # Cannot accommodate the job
        return None, []
    # No jobs or no free compute nodes
    return None, []
  
  
  def sort_rack_ids(self, rack_id, total_nracks):
    # Sort the rack id by the distance to the selected rack_id
    sorted_rack_id = []
    rack_ids_distance = {i:abs(i-rack_id) for i in range(total_nracks)}
    sorted_rack_ids_distance = dict(sorted(rack_ids_distance.items(),
                                           key=lambda item: item[1]))
    sorted_rack_id = list(sorted_rack_ids_distance.keys())
    return sorted_rack_id
        
  
  def resource_round_up(self, memory_granularity, x):
    return memory_granularity * (int(math.ceil(x/memory_granularity)))