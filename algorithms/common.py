import math
from operator import attrgetter


def load_balance_allocation(job, cluster):
  cnode_memory_capacity = cluster.compute_node_memory_capacity
  memory_granularity = cluster.memory_granularity
  compute_memory_node_tuples = [] #(compute_node, memory_node, remote_memory_capacity)
  
  racks = cluster.racks
  
  # First check if the cluster has enough compute nodes for the job
  if cluster.accommodate(job):
    candidate_nodes = []
    nnodes = job.nnodes
    
    # Step 1: allocate compute nodes from racks with less load
    racks.sort(key=attrgetter('number_of_free_compute_nodes'), reverse=True)
    for r in racks:
      if r.number_of_free_compute_nodes >= nnodes:
        # This rack has enough free compute nodes
        candidate_nodes.extend(r.free_compute_nodes[:nnodes])
        break
      else:
        # This rack does not have enough free compute nodes for this job,
        # allocate all free compute nodes in this rack and continue to the next rack
        candidate_nodes.extend(r.free_compute_nodes)
        nnodes -= r.number_of_free_compute_nodes

    # Step 2: allocate memory nodes if necessary
    # We have checked the disaggregation option in broker. If disaggregation is
    # unavailable, the jobs that need more memory than compute nodes are failed
    # and will not be in the waiting queue.
    if cnode_memory_capacity >= job.memory:
      # Compute nodes have enough memory resources, do not need memory node
      for c_node in candidate_nodes:
        compute_memory_node_tuples.append( (c_node, None, 0) )
      return job, compute_memory_node_tuples
    else:
      # Find remote memory
      job_remote_memory = job.memory - cnode_memory_capacity
      job_remote_memory_round_up = resource_round_up(memory_granularity, job_remote_memory)
      memory_node_capacity_records = {} # {Key: memory_node_id, Value: memory_node_free_memory}
      
      # For each compute node, find a memory node for it. 
      # Again we should allocate memory nodes with less load first.
      total_mnodes = cluster.total_memory_nodes
      total_mnodes.sort(key=attrgetter('free_memory'), reverse=True)
      
      for c_node in candidate_nodes:
        flag = False
        for m_node in total_mnodes:
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
        # Fail to find the memory node for computer node(s) in all memory nodes
        # This job cannot be accommodated now.
        if flag == False:
          return None, []
        
      # all compute nodes should find memory resources from their corresponding memory nodes
      return job, compute_memory_node_tuples
            
  # Cannot accommodate the job
  return None, []


def rack_awared_allocation(job, cluster):
  cnode_memory_capacity = cluster.compute_node_memory_capacity
  memory_granularity = cluster.memory_granularity
  compute_memory_node_tuples = [] #(compute_node, memory_node, remote_memory_capacity)
  
  racks = cluster.racks
  
  # First check if the cluster has enough compute nodes for the job
  if cluster.accommodate(job):
    candidate_nodes = []
    nnodes = job.nnodes
    
    # Step 1: allocate compute nodes from racks with less load
    racks.sort(key=attrgetter('number_of_free_compute_nodes'), reverse=True)
    for r in racks:
      if r.number_of_free_compute_nodes >= nnodes:
        # This rack has enough free compute nodes
        candidate_nodes.extend(r.free_compute_nodes[:nnodes])
        break
      else:
        # This rack does not have enough free compute nodes for this job,
        # allocate all free compute nodes in this rack and continue to the next rack
        candidate_nodes.extend(r.free_compute_nodes)
        nnodes -= r.number_of_free_compute_nodes
        
    # Step 2: allocate memory nodes if necessary
    # We have checked the disaggregation option in broker. If disaggregation is
    # unavailable, the jobs that need more memory than compute nodes are failed
    # and will not be in the waiting queue.
    if cnode_memory_capacity >= job.memory:
      # Compute nodes have enough memory resources, do not need memory node
      for c_node in candidate_nodes:
        compute_memory_node_tuples.append( (c_node, None, 0) )
      return job, compute_memory_node_tuples
    else:
      # Find remote memory
      job_remote_memory = job.memory - cnode_memory_capacity
      job_remote_memory_round_up = resource_round_up(memory_granularity, job_remote_memory)
      memory_node_capacity_records = {} # {Key: memory_node_id, Value: memory_node_free_memory}
      
      # For each compute node, find a memory node for it
      # We should allocate memory nodes close to compute nodes first.
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

        # Fail to find the memory node in the same rack, try other racks in the distance order
        if flag == False:
          sort_racks = sort_rack_ids(c_node_rack.id, len(racks))
          for rack_id in sort_racks[1:]:
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
      return job, compute_memory_node_tuples
    
  # Cannot accommodate the job
  return None, []


def rack_within_allocation(job, cluster):
  cnode_memory_capacity = cluster.compute_node_memory_capacity
  memory_granularity = cluster.memory_granularity
  compute_memory_node_tuples = [] #(compute_node, memory_node, remote_memory_capacity)
  
  racks = cluster.racks
  
  # First check if the cluster has enough compute nodes for the job
  if cluster.accommodate(job):
    candidate_nodes = []
    nnodes = job.nnodes
    
    # Step 1: allocate compute nodes from racks with less load
    racks.sort(key=attrgetter('number_of_free_compute_nodes'), reverse=True)
    for r in racks:
      if r.number_of_free_compute_nodes >= nnodes:
        # This rack has enough free compute nodes
        candidate_nodes.extend(r.free_compute_nodes[:nnodes])
        break
      else:
        # This rack does not have enough free compute nodes for this job,
        # allocate all free compute nodes in this rack and continue to the next rack
        candidate_nodes.extend(r.free_compute_nodes)
        nnodes -= r.number_of_free_compute_nodes
        
    # Step 2: allocate memory nodes if necessary
    # We have checked the disaggregation option in broker. If disaggregation is
    # unavailable, the jobs that need more memory than compute nodes are failed
    # and will not be in the waiting queue.
    if cnode_memory_capacity >= job.memory:
      # Compute nodes have enough memory resources, do not need memory node
      for c_node in candidate_nodes:
        compute_memory_node_tuples.append( (c_node, None, 0) )
      return job, compute_memory_node_tuples
    else:
      # Find remote memory
      job_remote_memory = job.memory - cnode_memory_capacity
      job_remote_memory_round_up = resource_round_up(memory_granularity, job_remote_memory)
      memory_node_capacity_records = {} # {Key: memory_node_id, Value: memory_node_free_memory}
      
      # For each compute node, find a memory node for it
      # We should allocate memory nodes within the same rack as compute nodes.
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

        # Fail to find the memory node in the same rack. We cannot allocate this job.
        if flag == False:
          job.fail()
          cluster.add_failed_jobs(job, 'out-of-memory')
          return None, []
        
      # all compute nodes should find their memory nodes
      return job, compute_memory_node_tuples
    
  # Cannot accommodate the job
  return None, []


def sort_rack_ids(rack_id, total_nracks):
  # Sort the rack id by the distance to the selected rack_id
  sorted_rack_id = []
  rack_ids_distance = {i:abs(i-rack_id) for i in range(total_nracks)}
  sorted_rack_ids_distance = dict(sorted(rack_ids_distance.items(),
                                          key=lambda item: item[1]))
  sorted_rack_id = list(sorted_rack_ids_distance.keys())
  return sorted_rack_id
  

def resource_round_up(memory_granularity, x):
  return memory_granularity * (int(math.ceil(x/memory_granularity)))