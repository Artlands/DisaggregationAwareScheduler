import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from matplotlib.figure import figaspect

ftsize = 12
plt.rcParams['axes.edgecolor']='#333F4B'
plt.rcParams['axes.linewidth']=0.8
plt.rcParams['xtick.color']='#333F4B'
plt.rcParams['ytick.color']='#333F4B'
plt.rcParams['font.size'] = ftsize

figsize=(12, 8)

def cdf_plot(all_dfs, metric, algorithms):
  if metric == 'turnaround':
    xlabel = 'Job Turnaround Time (End Time - Submit Time)'
  elif metric == 'waiting':
    xlabel = 'Job Response Time (Start Time - Submit Time)'
  elif metric == 'slowdown':
    xlabel = 'Job Performance Slowdown'
  else:
    xlabel = metric.replace('_', ' ').title()
  
  labels = []
  for n in algorithms:
    if n in['rack_scale', 'system_scale']:
      lname = n + '(backfill)'
    else:
      lname = n
    labels.append(lname)
    
  all_dfs_none_fails = []
  for df in all_dfs:
    df = df[df['failed'] == False]
    all_dfs_none_fails.append(df)
  
  all_slowdowns = []  
  for df in all_dfs_none_fails:
    df = df[df['slowdown'] != 0.0]
    all_slowdowns.append(df)
  
  selected_dfs = []
  if metric == 'slowdown':
    selected_dfs = all_slowdowns
  else:
    selected_dfs = all_dfs_none_fails
    
  all_cdfs = []
  for df in selected_dfs:
    series = df[metric]
    cdf = series.value_counts().sort_index()
    cdf = cdf/cdf.sum()
    cdf = cdf.cumsum()
    all_cdfs.append(cdf)
  
  w,h = figaspect(2/3)
  fig, ax = plt.subplots(figsize=(w,h))

  colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b','#e377c2', '#7f7f7f', '#bcbd22', '#17becf', '#1f78b4', '#33a02c']

  for i, cdf in enumerate(all_cdfs):
    ax.plot(cdf.index.to_numpy(), cdf.to_numpy(), label=labels[i], color=colors[i])

  ax.yaxis.grid(linestyle='--')
  ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))

  ax.set_ylabel("Cumulative Count")
  ax.set_xlabel(xlabel)
  ax.legend(frameon=False, fontsize=10)