import math
from operator import attrgetter
from core.algorithm import Algorithm


class RackFirstAlgorithm(Algorithm):
  def __call__(self, cluster, clock):
    """
    The Rack First Algorithm first checks if a rack can accommodate a job, if no rack is found, it finds the rack with most available nodes and remaining nodes will be selected from it's neighbor racks.
    The job's memory is served both on the assigned nodes (as local memory) and remote memory nodes (as remote memory) if its memory requirement exceeds the compute node memory capacity.
    """
    total_free_compute_nodes = cluster.total_free_compute_nodes
    compute_node_memory_capacity = cluster.compute_node_memory_capacity
    jobs = cluster.jobs_in_waiting_queue
    racks = cluster.racks
    racks.sort(key=attrgetter('number_of_free_compute_nodes'), reverse=True)
    top_rack = racks[0]
    candidate_job = None
    candidate_racks = []
    candidate_nodes = []
    number_tasks_on_racks = []
    candidate_memory_nodes = [] # {memory_node: MemoryNode, remote_memory: remote_memory}
    
    # print(f'Job queue: {len(jobs)}. Failed jobs: {len(cluster.failed_jobs)}')
    # print(f'Job queue: {[job.id for job in jobs]}')
    # print(f'Available computer nodes: {len(cluster.total_free_compute_nodes)}')
    
    if total_free_compute_nodes and jobs:
      for job in jobs:
        # First check if the cluster has enough available nodes for the job
        if cluster.accommodate(job):
          candidate_job = job
          # Allocate all available compute nodes from the top rack and the remaing required nodes are selected from it's neighbor racks. Note that the racks are already sorted by the number of available compute nodes.
          number_of_remaining_nodes = job.nnodes
          # Racks sorted by distance to the current rack
          sort_rack_ids = self.sort_rack_ids(top_rack.id, len(racks))
          for rack_id in sort_rack_ids:
            select_rack = cluster.find_rack(rack_id)
            compute_nodes = select_rack.free_compute_nodes
            compute_nodes.sort(key=attrgetter('id'))
            # Allocate compute nodes in the current rack as much as possible.
            number_of_allocated_nodes = min(len(compute_nodes), number_of_remaining_nodes)
            # Update candidate lists
            candidate_nodes.extend(compute_nodes[:number_of_allocated_nodes])
            candidate_racks.append(select_rack)
            number_tasks_on_racks.append(number_of_allocated_nodes)
            
            number_of_remaining_nodes -= number_of_allocated_nodes
            if number_of_remaining_nodes == 0:
              break
            
          # print(f'Allocated nodes: {[node.id for node in candidate_nodes]} for job {job.id}')
          # Should always be true since we've already checked the cluster nodes capacity and the job allocate enough free nodes.
          # assert number_of_remaining_nodes == 0
          # Successfully allocate compute nodes, now check the memory capacity requirement
          if compute_node_memory_capacity >= job.memory:
            break
          else:
            # print(f'Find available memory on memory node...')
            # Find the memory capacity in memory nodes. The remote memory is first served for the jobs on the same rack and then jobs on other racks.
            total_unassiged_remote_memory_units = 0 # Per task
            job_remote_memory = job.memory - compute_node_memory_capacity
            
            for idx, c_rack in enumerate(candidate_racks):
                number_tasks_on_this_rack = number_tasks_on_racks[idx]
                number_unassiged_remote_memory_units = number_tasks_on_this_rack
                memory_nodes = c_rack.memory_nodes
                memory_nodes.sort(key=attrgetter('free_memory'), reverse=True)

                # Allocate the remote memory to the job's tasks as much as possible.
                for memory_node in memory_nodes:
                  remote_memory_record = {'memory_node': memory_node, 'remote_memory': 0}
                  this_memory_node_free_memory = memory_node.free_memory
                  i = 0
                  while i < number_unassiged_remote_memory_units:
                    if this_memory_node_free_memory >= job_remote_memory:
                      # candidate_memory_nodes.append(memory_node)
                      remote_memory_record['remote_memory'] += job_remote_memory
                      this_memory_node_free_memory -=  job_remote_memory
                      i += 1
                    else:
                      # The remote memory on this memory node are exhausted.
                      break
                  number_unassiged_remote_memory_units -= i
                  
                  candidate_memory_nodes.append(remote_memory_record)
                  # All remote memory of the job's tasks are satisfied
                  if number_unassiged_remote_memory_units == 0:
                    break
                
                # If there are remote memory units are not satisfied, save the count and serve them later
                total_unassiged_remote_memory_units += number_unassiged_remote_memory_units
                
            # print(f'Unassigned remote memory units {total_unassiged_remote_memory_units}')
            # Use the memory nodes on the rest of racks to serve the remaining unassigned remote memory units
            if total_unassiged_remote_memory_units > 0:
              other_racks = [rack for rack in racks if rack not in candidate_racks]
              other_racks.sort(key=attrgetter('free_remote_memory'), reverse=True)
              for rack in other_racks:
                memory_nodes = rack.memory_nodes
                memory_nodes.sort(key=attrgetter('free_memory'), reverse=True)
                for memory_node in memory_nodes:
                  remote_memory_record = {'memory_node': memory_node, 'remote_memory': 0}
                  this_memory_node_free_memory = memory_node.free_memory
                  i = 0
                  while i < total_unassiged_remote_memory_units:
                    if this_memory_node_free_memory >= job_remote_memory:
                      # candidate_memory_nodes.append(memory_node)
                      remote_memory_record['remote_memory'] += job_remote_memory
                      this_memory_node_free_memory -=  job_remote_memory
                      i += 1
                    else:
                      # The remote memory on this memory node are exhausted.
                      break
                  total_unassiged_remote_memory_units -= i
                  
                  candidate_memory_nodes.append(remote_memory_record)
                  if total_unassiged_remote_memory_units == 0:
                    break
            
            if total_unassiged_remote_memory_units != 0:
              # Unable to find enough remote memory for the job
              candidate_job = None
              candidate_nodes = []
              candidate_memory_nodes = []
        return candidate_job, candidate_nodes, candidate_memory_nodes
    return candidate_job, candidate_nodes, candidate_memory_nodes

  def sort_rack_ids(self, rack_id, total_nracks):
    # Sort the rack id by the distance to the selected rack_id
    sorted_rack_id = []
    rack_ids_distance = {i:abs(i-rack_id) for i in range(total_nracks)}
    sorted_rack_ids_distance = dict(sorted(rack_ids_distance.items(),
                                           key=lambda item: item[1]))
    sorted_rack_id = list(sorted_rack_ids_distance.keys())
    return sorted_rack_id
  