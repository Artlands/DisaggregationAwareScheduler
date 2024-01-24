import math
import random
import operator
from operator import attrgetter


def system_balance_allocation(job, cluster):
  """ This allocation policy try to balance the load of compute nodes and the 
      load of memory nodes across racks. However, it does not consider the remote 
      memory locality.
  """
  cnode_memory_capacity = cluster.compute_node_memory_capacity
  memory_granularity = cluster.memory_granularity
  compute_memory_node_tuples = [] #[(compute_node, memory_node, remote_memory_capacity)]
  cross_rack_allocation_count = 0
  cross_rack_allocation_capacity = 0
  racks = cluster.racks
  
  # First check if the cluster has enough compute nodes for the job
  if cluster.accommodate(job):
    candidate_nodes = []
    nnodes = job.nnodes
    
    # Step 1: allocate compute nodes from racks with less load. This allocation
    # strategy is try to condense the allocated compute nodes in as few racks as possible.
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
    if cnode_memory_capacity >= job.max_memory:
      # Compute nodes have enough memory resources, do not need memory node
      for c_node in candidate_nodes:
        compute_memory_node_tuples.append( (c_node, None, 0) )
      return job, compute_memory_node_tuples
    else:
      # Find remote memory
      job_remote_memory = job.max_memory - cnode_memory_capacity
      job_remote_memory_round_up = resource_round_up(memory_granularity, job_remote_memory)
      
      # Keep a record of the free memory of each memory node
      memory_node_capacity_records = [] # [(memory_node_id, memory_node_free_memory)]
      for m_node in cluster.total_memory_nodes:
        memory_node_capacity_records.append( (m_node.id, m_node.free_memory) )
        
      # Sort the memory nodes by free memory in descending order
      memory_node_capacity_records.sort(key=operator.itemgetter(1), reverse=True)
      
      for c_node in candidate_nodes:
        c_node_rack_id = c_node.rack.id
        flag = False
        for i, (m_node_id, m_node_free_memory) in enumerate(memory_node_capacity_records):
          if m_node_free_memory >= job_remote_memory_round_up:
            memory_node_capacity_records[i] = (m_node_id, m_node_free_memory - job_remote_memory_round_up)
            m_node = cluster.find_memory_node(m_node_id)
            m_node_rack_id = m_node.rack.id
            compute_memory_node_tuples.append( (c_node, m_node, job_remote_memory_round_up) )
            # Update the cross rack allocation statistics
            if(c_node_rack_id != m_node_rack_id):
              cross_rack_allocation_count += 1
              cross_rack_allocation_capacity += job_remote_memory_round_up
            flag = True
            break            

        # Fail to find the memory node for computer node(s) in all memory nodes
        # This job cannot be allocated now.
        if flag == False:
          return None, []
        
      # At this point, all compute nodes should find memory resources from their corresponding memory nodes
      # Update the cross rack allocation statistics
      job.add_statistic(cross_rack_allocation_count, cross_rack_allocation_capacity)
      return job, compute_memory_node_tuples
            
  # Cannot accommodate the job because of the lack of compute nodes
  return None, []


def system_random_allocation(job, cluster):
  """ This allocation policy randomly assigns compute nodes and memory nodes to jobs.
  """
  cnode_memory_capacity = cluster.compute_node_memory_capacity
  memory_granularity = cluster.memory_granularity
  compute_memory_node_tuples = [] #[(compute_node, memory_node, remote_memory_capacity)]
  cross_rack_allocation_count = 0
  cross_rack_allocation_capacity = 0
  racks = cluster.racks
  
  # First check if the cluster has enough compute nodes for the job
  if cluster.accommodate(job):
    candidate_nodes = []
    nnodes = job.nnodes
    
    # Step 1: randomly allocate compute nodes from racks.
    # random.shuffle(racks)
    all_free_compute_nodes = []
    
    for r in racks:
      all_free_compute_nodes.extend(r.free_compute_nodes)
      # if r.number_of_free_compute_nodes >= nnodes:
      #   # This rack has enough free compute nodes
      #   candidate_nodes.extend(r.free_compute_nodes[:nnodes])
      #   break
      # else:
      #   # This rack does not have enough free compute nodes for this job,
      #   # allocate all free compute nodes in this rack and continue to the next rack
      #   candidate_nodes.extend(r.free_compute_nodes)
      #   nnodes -= r.number_of_free_compute_nodes

    random.shuffle(all_free_compute_nodes)
    candidate_nodes = all_free_compute_nodes[:nnodes]
    
    # Step 2: allocate memory nodes if necessary
    # We have checked the disaggregation option in broker. If disaggregation is
    # unavailable, the jobs that need more memory than compute nodes are failed
    # and will not be in the waiting queue.
    if cnode_memory_capacity >= job.max_memory:
      # Compute nodes have enough memory resources, do not need memory node
      for c_node in candidate_nodes:
        compute_memory_node_tuples.append( (c_node, None, 0) )
      return job, compute_memory_node_tuples
    else:
      # Find remote memory
      job_remote_memory = job.max_memory - cnode_memory_capacity
      job_remote_memory_round_up = resource_round_up(memory_granularity, job_remote_memory)
      
      # Keep a record of the free memory of each memory node
      memory_node_capacity_records = [] # [(memory_node_id, memory_node_free_memory)]
      for m_node in cluster.total_memory_nodes:
        memory_node_capacity_records.append( (m_node.id, m_node.free_memory) )

      # Shuffle the memory nodes
      random.shuffle(memory_node_capacity_records)
      
      for c_node in candidate_nodes:
        c_node_rack_id = c_node.rack.id
        flag = False
        for i, (m_node_id, m_node_free_memory) in enumerate(memory_node_capacity_records):
          if m_node_free_memory >= job_remote_memory_round_up:
            memory_node_capacity_records[i] = (m_node_id, m_node_free_memory - job_remote_memory_round_up)
            m_node = cluster.find_memory_node(m_node_id)
            m_node_rack_id = m_node.rack.id
            compute_memory_node_tuples.append( (c_node, m_node, job_remote_memory_round_up) )
            # Update the cross rack allocation statistics
            if(c_node_rack_id != m_node_rack_id):
              cross_rack_allocation_count += 1
              cross_rack_allocation_capacity += job_remote_memory_round_up
            flag = True
            break            

        # Fail to find the memory node for computer node(s) in all memory nodes
        # This job cannot be allocated now.
        if flag == False:
          return None, []
        
      # At this point, all compute nodes should find memory resources from their corresponding memory nodes
      # Update the cross rack allocation statistics
      job.add_statistic(cross_rack_allocation_count, cross_rack_allocation_capacity)
      return job, compute_memory_node_tuples
            
  # Cannot accommodate the job because of the lack of compute nodes
  return None, []


def rack_balance_allocation(job, cluster):
  """ This allocation policy try to balance the load of compute nodes and the 
      load of memory nodes across racks, and it considers the remote memory locality.
  """
  cnode_memory_capacity = cluster.compute_node_memory_capacity
  memory_granularity = cluster.memory_granularity
  compute_memory_node_tuples = [] #(compute_node, memory_node, remote_memory_capacity)
  racks = cluster.racks
  flag = False # Flag the allocation status
  
  # First check if the cluster has enough compute nodes for the job
  if cluster.accommodate(job):
    candidate_nodes = []
    nnodes = job.nnodes
    # Sort the racks by the available compute nodes in descending order
    racks.sort(key=attrgetter('number_of_free_compute_nodes'), reverse=True)
    
    if cnode_memory_capacity >= job.max_memory:
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
      for c_node in candidate_nodes:
        compute_memory_node_tuples.append( (c_node, None, 0) )
      return job, compute_memory_node_tuples
    else:
      job_remote_memory = job.max_memory - cnode_memory_capacity
      job_remote_memory_round_up  = resource_round_up(memory_granularity, job_remote_memory)
      
      for r in racks:
        rack_free_compute_nodes = r.free_compute_nodes
        rack_memory_node_capacity_records = [] # [(memory_node_id, memory_node_free_memory)]
        for m_node in r.memory_nodes:
          rack_memory_node_capacity_records.append( (m_node.id, m_node.free_memory) )
        # Sort the memory nodes by free memory in descending order
        rack_memory_node_capacity_records.sort(key=operator.itemgetter(1), reverse=True)
                
        for (m_node_id, m_node_free_memory) in rack_memory_node_capacity_records:
          while (m_node_free_memory >= job_remote_memory_round_up) and nnodes > 0:
            if (len(rack_free_compute_nodes) > 0):
              c_node = rack_free_compute_nodes.pop(0)
              m_node = cluster.find_memory_node(m_node_id)
              compute_memory_node_tuples.append( (c_node, m_node, job_remote_memory_round_up) )
              m_node_free_memory -= job_remote_memory_round_up
              nnodes -= 1
            else:
              break

        # Found enough compute nodes and corresponding remote memories for this job
        if nnodes == 0:
          flag = True
          break
      
      if flag == False:
        # At this point, even though we have enough compute nodes, we cannot find
        # remote memory resources for this job. Stall the job and delay its submit time
        job.stall += 1
        job.submit += 1
        return None, []
      else:
        return job, compute_memory_node_tuples
      
  # Cannot accommodate the job
  return None, []
  

def rack_random_allocation(job, cluster):
  """ This allocation policy randomly assigns the compute nodes and the memory
      nodes in its corresponding rack, and it considers the remote memory locality.
  """
  cnode_memory_capacity = cluster.compute_node_memory_capacity
  memory_granularity = cluster.memory_granularity
  compute_memory_node_tuples = [] #(compute_node, memory_node, remote_memory_capacity)
  racks = cluster.racks
  flag = False # Flag the allocation status
  
  # First check if the cluster has enough compute nodes for the job
  if cluster.accommodate(job):
    candidate_nodes = []
    nnodes = job.nnodes
    
    # Shuffle the racks
    random.shuffle(racks)
    
    if cnode_memory_capacity >= job.max_memory:
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
      for c_node in candidate_nodes:
        compute_memory_node_tuples.append( (c_node, None, 0) )
      return job, compute_memory_node_tuples
    else:
      job_remote_memory = job.max_memory - cnode_memory_capacity
      job_remote_memory_round_up  = resource_round_up(memory_granularity, job_remote_memory)
      
      for r in racks:
        rack_free_compute_nodes = r.free_compute_nodes
        rack_memory_node_capacity_records = [] # [(memory_node_id, memory_node_free_memory)]
        for m_node in r.memory_nodes:
          rack_memory_node_capacity_records.append( (m_node.id, m_node.free_memory) )
        
        # Shuffle the memory nodes
        random.shuffle(rack_memory_node_capacity_records)
                
        for (m_node_id, m_node_free_memory) in rack_memory_node_capacity_records:
          while (m_node_free_memory >= job_remote_memory_round_up) and nnodes > 0:
            if (len(rack_free_compute_nodes) > 0):
              c_node = rack_free_compute_nodes.pop(0)
              m_node = cluster.find_memory_node(m_node_id)
              compute_memory_node_tuples.append( (c_node, m_node, job_remote_memory_round_up) )
              m_node_free_memory -= job_remote_memory_round_up
              nnodes -= 1
            else:
              break

        # Found enough compute nodes and corresponding remote memories for this job
        if nnodes == 0:
          flag = True
          break
      
      if flag == False:
        # At this point, even though we have enough compute nodes, we cannot find
        # remote memory resources for this job. Stall the job and delay its submit time
        job.stall += 1
        job.submit += 1
        return None, []
      else:
        return job, compute_memory_node_tuples
      
  # Cannot accommodate the job
  return None, []


def rack_memory_aware_allocation(job, cluster):
  """ This allocation policy only considers the rack-scale memory disaggregation.
      For jobs only require local memory, they should be allocated to racks with
      the maximum allocated remote memory to fully utilize the compute nodes.
      
      For jobs use remote memory, this policy will try to allocate jobs to racks 
      with minimum allocated remote memory (i.e. maximum free remote memory) to
      reduce the variation of remote memory capacity among racks. 
      The allocated compute nodes in the rack will be: 
        min(available compute nodes, available remote memory slots)
  """
  cnode_memory_capacity = cluster.compute_node_memory_capacity
  memory_granularity = cluster.memory_granularity
  compute_memory_node_tuples = [] #(compute_node, memory_node, remote_memory_capacity)
  racks = cluster.racks
  flag = False # Flag the allocation status
  
  # First check if the cluster has enough compute nodes for the job
  if cluster.accommodate(job):
    candidate_nodes = []
    nnodes = job.nnodes
    
    if cnode_memory_capacity >= job.max_memory:
      racks.sort(key=attrgetter('free_remote_memory'))
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
      for c_node in candidate_nodes:
        compute_memory_node_tuples.append( (c_node, None, 0) )
      return job, compute_memory_node_tuples
    else:
      racks.sort(key=attrgetter('free_remote_memory'), reverse=True)
      job_remote_memory = job.max_memory - cnode_memory_capacity
      job_remote_memory_round_up  = resource_round_up(memory_granularity, job_remote_memory)
      
      for r in racks:
        rack_free_compute_nodes = r.free_compute_nodes
        rack_memory_node_capacity_records = [] # [(memory_node_id, memory_node_free_memory)]
        for m_node in r.memory_nodes:
          rack_memory_node_capacity_records.append( (m_node.id, m_node.free_memory) )
        # Sort the memory nodes by free memory in descending order
        rack_memory_node_capacity_records.sort(key=operator.itemgetter(1), reverse=True)
                
        for (m_node_id, m_node_free_memory) in rack_memory_node_capacity_records:
          while (m_node_free_memory >= job_remote_memory_round_up) and nnodes > 0:
            if (len(rack_free_compute_nodes) > 0):
              c_node = rack_free_compute_nodes.pop(0)
              m_node = cluster.find_memory_node(m_node_id)
              compute_memory_node_tuples.append( (c_node, m_node, job_remote_memory_round_up) )
              m_node_free_memory -= job_remote_memory_round_up
              nnodes -= 1
            else:
              break

        # Found enough compute nodes and corresponding remote memories for this job
        if nnodes == 0:
          flag = True
          break
      
      if flag == False:
        # At this point, even though we have enough compute nodes, we cannot find
        # remote memory resources for this job. Stall the job and delay its submit time
        job.stall += 1
        job.submit += 1
        return None, []
      else:
        return job, compute_memory_node_tuples
      
  # Cannot accommodate the job
  return None, []


def backfill_plan(front_job, candidate_jobs, cluster, clock):
  # This implements the EASY backfilling algorithm
  nnodes_required = front_job.nnodes
  running_jobs = cluster.running_jobs

  jobs_time_to_completion = [] # [(job_id, time_to_completion)]
  for job in running_jobs:
    job_id = job.id
    time_to_completion = job.duration - (clock - job.start)
    jobs_time_to_completion.append( (job_id, time_to_completion) )
  
  # Sort the jobs by time to completion in ascending order
  jobs_time_to_completion.sort(key=operator.itemgetter(1))
  
  # Iterate through the sorted running jobs to find the available future released nodes
  flag = False
  total_future_released_nodes = 0
  for job_id, time_to_completion in jobs_time_to_completion:
    job = cluster.find_job(job_id)
    total_future_released_nodes += job.nnodes
    total_available_nodes = total_future_released_nodes + len(cluster.total_free_compute_nodes)
    if(total_available_nodes >= nnodes_required):
      # The front job can be scheduled after the earliest job(s) finish
      flag = True
      break
    else:
      # move to the next running job
      continue
    
  if flag == True:
    # The candidate job should be finished before the time to completion of the earliest job(s)
    # Otherwise, the candidate job cannot be scheduled
    for candidate_job in candidate_jobs:
      if candidate_job.duration <= time_to_completion:
        # The candidate job can be scheduled
        return candidate_job
  
  return None


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