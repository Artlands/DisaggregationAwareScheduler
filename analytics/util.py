import json
import pandas as pd


def find_range(df_t, pct = 0.01):
  # Order the dataframe by the start time
  df_t.sort_values(by=['start'], inplace=False, ascending=True)

  # Get rows of the dataframe
  total_jobs = df_t.shape[0]

  first_1_percent = int(total_jobs * pct)

  # Start time of the first 1% of the jobs
  start = df_t.iloc[first_1_percent]['start']

  # Start time of the last arrived job
  end = df_t.iloc[-1]['start']
  
  return start, end


def bsld(waiting_time, duration):
  return max((waiting_time + duration) / max(duration, 10), 1)


def load_monitoring(sub_dir, dataset, algo, alloc, mnode_cnt, cnode_mcap=256, cnode_cnt=256, mnode_mcap=1024, bf='true', filter=True, range_pct=0.1):
  job_trace = None
  cluster_trace = None
  start = 0
  end = 0
  try:
    # Load the job summary file
    with open(f'../monitoring/{sub_dir}/{dataset}/job_summary_J0-0_C{cnode_cnt}-{cnode_mcap}GB_M{mnode_cnt}-{mnode_mcap}GB_{algo}-{alloc}_BF-{bf}.json', 'r') as f:
      trace = json.loads(f.read())
      df_trace = pd.DataFrame.from_dict(trace, orient='index')
      # Only keep successful jobs
      df_trace = df_trace[df_trace['failed'] == False]
      
      df_trace['duration'] = df_trace['finish'] - df_trace['start']
      df_trace['waiting_time'] = df_trace['start'] - df_trace['submit'] + df_trace['stall'] # (submit - stall) is the real submit time
      df_trace['response_time'] = df_trace['finish'] - df_trace['submit'] + df_trace['stall']
      df_trace['fairness'] = df_trace['waiting_time']/df_trace['duration']
      df_trace['bounded_slowdown'] = df_trace.apply(lambda row: bsld(row['waiting_time'], row['duration']), axis=1)
      if filter:
        start, end = find_range(df_trace, range_pct)
        df_trace = df_trace[(df_trace['start'] >= start) & (df_trace['finish'] <= end)]
      job_trace = df_trace
  except:
    print(f'Failed to load ../monitoring/{sub_dir}/{dataset}/job_summary_J0-0_C{cnode_cnt}-{cnode_mcap}GB_M{mnode_cnt}-{mnode_mcap}GB_{algo}-{alloc}_BF-{bf}.json')
    exit(1)
    
  try:
    with open(f'../monitoring/{sub_dir}/{dataset}/cluster_state_J0-0_C{cnode_cnt}-{cnode_mcap}GB_M{mnode_cnt}-{mnode_mcap}GB_{algo}-{alloc}_BF-{bf}.json', 'r') as f:
      trace = json.loads(f.read())
      df_trace = pd.json_normalize(trace)
      if filter:
        df_trace = df_trace[(df_trace['timestamp'] >= start) & (df_trace['timestamp'] <= end)]
      cluster_trace = df_trace
  except:
    print(f'Failed to load ../monitoring/{sub_dir}/{dataset}/cluster_state_J0-0_C{cnode_cnt}-{cnode_mcap}GB_M{mnode_cnt}-{mnode_mcap}GB_{algo}-{alloc}_BF-{bf}.json')
    exit(1)
  
  return job_trace, cluster_trace


def get_metric_list(metric, df_job_list):
  metric_list = []
  for df in df_job_list:
    avg = [df[i][metric].mean() for i in range(len(df))]
    metric_list.append(avg)
  return metric_list

def get_metric_list_median(metric, df_job_list):
  metric_list = []
  for df in df_job_list:
    avg = [df[i][metric].median() for i in range(len(df))]
    metric_list.append(avg)
  return metric_list

def get_num_records(df_job_list):
  metric_list = []
  for df in df_job_list:
    n = df.shape[0]
    metric_list.append(n)
  return metric_list

