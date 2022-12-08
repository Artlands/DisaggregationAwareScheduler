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
    memory_granularity = cluster.racks[0].compute_nodes[0].memory_granularity
    jobs = cluster.jobs_in_waiting_queue
    racks = cluster.racks
    racks.sort(key=attrgetter('number_of_free_compute_nodes'), reverse=True)
    top_rack = racks[0]
    candidate_job = None
    candidate_racks = []
    candidate_nodes = []
    number_tasks_on_racks = []
    candidate_memory_nodes = [] # {memory_node: MemoryNode, remote_memory: remote_memory}
    candidate_memory_nodes_dict = {} # {memory_node_id: {memory_node: MemoryNode, remote_memory: remote_memory} }
    
    # print(f'Job queue: {len(jobs)}. Failed jobs: {len(cluster.failed_jobs)}')
    # print(f'Job queue: {[job.id for job in jobs]}')
    # print(f'Available computer nodes: {len(cluster.total_free_compute_nodes)}')
    
    if total_free_compute_nodes and jobs:
      for job in jobs:
        # First check if the cluster has enough available nodes for the job
        if cluster.accommodate(job):
          candidate_job = job
          # Allocate all available compute nodes from the top rack and the remaing required nodes are selected from it's neighbor racks. The racks are already sorted by the number of available compute nodes.
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
            if number_of_remaining_nodes > 0:
              candidate_nodes.extend(compute_nodes[:number_of_allocated_nodes])
              candidate_racks.append(select_rack)
              number_tasks_on_racks.append(number_of_allocated_nodes)
              number_of_remaining_nodes -= number_of_allocated_nodes
                
            if number_of_remaining_nodes == 0:
              break

          # Should always be true since we've already checked the cluster nodes capacity and the job allocate enough free nodes.
          # assert number_of_remaining_nodes == 0
          
          # Successfully allocate compute nodes, now check the memory capacity requirement
          if compute_node_memory_capacity >= job.memory:
            # Job memory can be fully satisfied by local memory
            break
          else:
            # print(f'Find available memory on memory node...')
            # Find the memory capacity in memory nodes. The remote memory is first served for the jobs on the same rack and then jobs on other racks.
            total_unassigned_remote_memory_units = job.nnodes # Per task
            job_remote_memory = job.memory - compute_node_memory_capacity
            job_remote_memory_round_up = self.resource_round_up(memory_granularity, job_remote_memory)
            
            memory_nodes_still_have_free_slots = []
            for idx, c_rack in enumerate(candidate_racks):
                number_tasks_on_this_rack = number_tasks_on_racks[idx]
                assigned_memory_units_on_this_rack = 0
                memory_nodes = c_rack.memory_nodes
                memory_nodes.sort(key=attrgetter('free_memory'), reverse=True)
                
                # Allocate the remote memory to the job's tasks as much as possible.
                for memory_node in memory_nodes:
                  remote_memory_record = {'memory_node': memory_node, 'remote_memory': 0}
                  this_memory_node_free_memory = memory_node.free_memory
                  
                  while this_memory_node_free_memory >= job_remote_memory_round_up:
                    if assigned_memory_units_on_this_rack == number_tasks_on_this_rack:
                      break
                    else:
                      remote_memory_record['remote_memory'] += job_remote_memory_round_up
                      this_memory_node_free_memory -=  job_remote_memory_round_up
                      assigned_memory_units_on_this_rack += 1
                  
                  if remote_memory_record['remote_memory'] != 0:
                    candidate_memory_nodes_dict.update({
                      memory_node.id: remote_memory_record
                    })
                    
                  if this_memory_node_free_memory >= job_remote_memory_round_up:
                    free_slot = {'memory_node': memory_node, 'free_memory': this_memory_node_free_memory}
                    memory_nodes_still_have_free_slots.append(free_slot)

                total_unassigned_remote_memory_units -= assigned_memory_units_on_this_rack
                
                if total_unassigned_remote_memory_units == 0:
                    break # Break candidate_rack for loop
                
            # If there are remote memory units are not satisfied, save the count and serve them later
            
            # Use free memory slots on allocated racks first
            if (total_unassigned_remote_memory_units > 0) and (len(memory_nodes_still_have_free_slots) > 0):
              for free_slot in memory_nodes_still_have_free_slots:
                memory_node = free_slot['memory_node']
                free_memory = free_slot['free_memory']
                
                if memory_node.id in candidate_memory_nodes_dict:
                  remote_memory_record = candidate_memory_nodes_dict[memory_node.id]
                else:
                  remote_memory_record = {'memory_node': memory_node, 'remote_memory': 0}
                
                while total_unassigned_remote_memory_units > 0:
                  if free_memory >= job_remote_memory_round_up:
                    remote_memory_record['remote_memory'] += job_remote_memory_round_up
                    free_memory -= job_remote_memory_round_up
                    total_unassigned_remote_memory_units -= 1
                  else:
                    break
                
                candidate_memory_nodes_dict.update({
                  memory_node.id: remote_memory_record
                })
                  
                if total_unassigned_remote_memory_units == 0:
                  break # Break memory for loop
                                  
            # Use the memory nodes on the rest of racks to serve the remaining unassigned remote memory units
            if total_unassigned_remote_memory_units > 0:
              other_racks = [rack for rack in racks if rack not in candidate_racks]
              other_racks.sort(key=attrgetter('free_remote_memory'), reverse=True)
              for rack in other_racks:
                memory_nodes = rack.memory_nodes
                memory_nodes.sort(key=attrgetter('free_memory'), reverse=True)
                for memory_node in memory_nodes:
                  remote_memory_record = {'memory_node': memory_node, 'remote_memory': 0}
                  this_memory_node_free_memory = memory_node.free_memory
                  
                  while total_unassigned_remote_memory_units > 0:
                    if this_memory_node_free_memory >= job_remote_memory_round_up:
                      remote_memory_record['remote_memory'] += job_remote_memory_round_up
                      this_memory_node_free_memory -= job_remote_memory_round_up
                      total_unassigned_remote_memory_units -= 1
                    else:
                      break
                  
                  if remote_memory_record['remote_memory'] != 0:
                    # candidate_memory_nodes.append(remote_memory_record)
                    candidate_memory_nodes_dict.update({
                      memory_node.id: remote_memory_record
                    })
                    
                  if total_unassigned_remote_memory_units == 0:
                    break # Break memory for loop
                  
                if total_unassigned_remote_memory_units == 0:
                    break # Break rack for loop
                
            if total_unassigned_remote_memory_units != 0:
              # print(f'Cannot allocate remote memory for job: {job.id}')
              candidate_job = None
              candidate_nodes = []
              candidate_memory_nodes = []
            else:
              candidate_memory_nodes = list(candidate_memory_nodes_dict.values())
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
  
  def resource_round_up(self, memory_granularity, x):
    return memory_granularity * (int(math.ceil(x/memory_granularity)))
  