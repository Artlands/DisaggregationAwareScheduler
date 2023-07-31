import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from matplotlib.figure import figaspect
import numpy as np

ftsize = 12
plt.rcParams['axes.edgecolor']='#333F4B'
plt.rcParams['axes.linewidth']=0.8
plt.rcParams['xtick.color']='#333F4B'
plt.rcParams['ytick.color']='#333F4B'
plt.rcParams['font.size'] = ftsize

figsize=(12, 8)
colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b','#e377c2', '#7f7f7f', '#bcbd22', '#17becf', '#1f78b4', '#33a02c']

def ts_plot(all_dfs, metric, tracenames, save=False):
  ylabel = metric.replace('_', ' ').title()
  labels = tracenames

  w,h = figaspect(2/3)
  fig, ax = plt.subplots(figsize=(w,h))

  if len(all_dfs) <= len(colors):
    for i, df in enumerate(all_dfs):
      ax.plot(df['timestamp'].to_numpy(), df[f'cluster_state.{metric}'].to_numpy(), label=labels[i], color=colors[i])
  else:
    for i, df in enumerate(all_dfs):
      ax.plot(df['timestamp'].to_numpy(), df[f'cluster_state.{metric}'].to_numpy(), label=labels[i])
  
  ax.yaxis.grid(linestyle='--')

  ax.set_ylabel(f"{ylabel}")
  ax.set_xlabel("Time")
  ax.legend(frameon=False, fontsize=10)
  # Save figure
  if save:
    plt.savefig(f'./figures/{metric}.png', dpi=300, bbox_inches='tight')
  

def line_comp_plot(lists, names, xticks, title, baseline = None, yscale='linear', save=False):  
  w,h = figaspect(2/3)
  fig, ax0 = plt.subplots(figsize=(w,h))
  ax1 = ax0.twinx()

  ax0.plot(xticks, lists[0], label=names[0], color=colors[1])
  ax1.plot(xticks, lists[1], label=names[1], color=colors[2])
  
  if baseline:
    # plot horizontal line
    ax1.axhline(y=baseline, color=colors[0], linestyle='--', label='baseline')
  
  ax0.yaxis.grid(linestyle='--')
  
  ax0.set_ylabel(names[0])
  ax1.set_ylabel(names[1])
  ax1.set_yscale(yscale)

  ax0.set_xlabel("Memory Node Capacity (TB) per Rack")
  # ax0.legend(frameon=False, fontsize=10)
  # ax1.legend(frameon=False, fontsize=10)
  lines0, labels0 = ax0.get_legend_handles_labels()
  lines1, labels1 = ax1.get_legend_handles_labels()
  ax1.legend(lines1 + lines0, labels1 + labels0, frameon=False, fontsize=10)
  plt.title(title)
  if save:
    plt.savefig(f'./figures/{names[0]}-{names[1]}.png', dpi=300, bbox_inches='tight')
  

def line_comp_plot_rs(lists, tracenames, xticks, title, ylabel, yscale='linear', baseline = None, save=False):  
  w,h = figaspect(2/3)
  fig, ax = plt.subplots(figsize=(w,h))
  
  for i, df in enumerate(lists):
    ax.plot(xticks, df, label=tracenames[i], color=colors[i+1])
  
  if baseline:
    # plot horizontal line
    ax.axhline(y=baseline, color=colors[0], linestyle='--', label='baseline')
  
  ax.yaxis.grid(linestyle='--')  
  ax.set_ylabel(ylabel)
  ax.set_yscale(yscale)

  ax.set_xlabel("Memory Node Capacity (TB) per Rack")
  ax.legend(frameon=False, fontsize=10)
  plt.title(title)
  if save:
    fname = ylabel.replace(' ', '-')
    plt.savefig(f'./figures/{fname}.png', dpi=300, bbox_inches='tight')

def line_single_plot(lists, names, xticks, title, baseline = None, yscale='linear'):  
  w,h = figaspect(2/3)
  fig, ax0 = plt.subplots(figsize=(w,h))

  ax0.plot(xticks, lists[0], label=names[0], color=colors[1])
  
  if baseline:
    # plot horizontal line
    ax0.axhline(y=baseline, color=colors[0], linestyle='--', label=f'baseline ({baseline:.2f})')
    # convert baseline to floating point with two decimal places
    
  
  ax0.yaxis.grid(linestyle='--')
  
  ax0.set_ylabel(names[0])
  ax0.set_yscale(yscale)
  ax0.set_xlabel("Memory Node Capacity (TB) per Rack")
  ax0.legend(frameon=False, fontsize=10)
  plt.title(title)
  


def cdf_plot(all_dfs, metric, tracenames, save=False):
  if metric == 'turnaround':
    xlabel = 'Job Turnaround Time (End Time - Submit Time)'
  elif metric == 'waiting':
    xlabel = 'Job Response Time (Start Time - Submit Time)'
  elif metric == 'slowdown':
    xlabel = 'Job Performance Slowdown'
  else:
    xlabel = metric.replace('_', ' ').title()
  
  labels = tracenames
    
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

  if len(all_cdfs) <= len(colors):
    for i, cdf in enumerate(all_cdfs):
      ax.plot(cdf.index.to_numpy(), cdf.to_numpy(), label=labels[i], color=colors[i])
  else:
    for i, cdf in enumerate(all_cdfs):
      ax.plot(cdf.index.to_numpy(), cdf.to_numpy(), label=labels[i])
      
  ax.yaxis.grid(linestyle='--')
  ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))

  ax.set_ylabel("Cumulative Count")
  ax.set_xlabel(xlabel)
  ax.legend(frameon=False, fontsize=10)
  if save:
    plt.savefig(f'./figures/{metric}.png', dpi=300, bbox_inches='tight')
  
  
def bar_plot(all_dfs, warmup_threshold, tracenames, metric, ylabel, save=False):
  w,h = figaspect(2/3)
  fig, ax = plt.subplots(figsize=(w,h))

  ecolor = '#000000'
  colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b','#e377c2', '#bcbd22', '#17becf', '#1f78b4', '#33a02c']
  hatchs = ['xxx', '///', '---', 'ooo', '...', '+++', '***', '\\\\\\']
  alpha = 1
  width = 0.3

  throughputs = []
  for i, df in enumerate(all_dfs):
    try:
      df = df[df['timestamp'] >= warmup_threshold]
    except:
      df = df[df['submit'] >= warmup_threshold]
    series = df[metric]
    running_jobs = series.to_numpy()
    throughput = running_jobs.mean()
    throughputs.append(throughput)
    ax.bar(i, throughput, color=colors[i], width=width, edgecolor=ecolor, hatch = hatchs[i], alpha = alpha, label=tracenames[i])
  # throughput

  # Set x tick labels
  ax.set_xticks(range(len(tracenames)))
  ax.set_xticklabels(tracenames)
  ax.yaxis.grid(linestyle='--')
  ax.set_ylabel(ylabel)
  ax.set_xlabel("Scheduling Algorithms")
  if save:
    plt.savefig(f'./figures/{ylabel}.png', dpi=300, bbox_inches='tight')
  

def boxplot(metric, idx, df_baseline_job, df_job_list, comp_list, save=False):
  tracenames = []
  ylabel = metric.replace('.', ' ').replace('_', ' ').title()
  
  data = []
  if df_baseline_job is not None:
    baseline = df_baseline_job[metric]
    data.append(baseline)
    tracenames.append('baseline')

  for i, df in enumerate(df_job_list):
    data.append(df[idx][metric])
    tracenames.append(f'{comp_list[i]}')

  w,h = figaspect(2/3)
  fig, ax = plt.subplots(figsize=(w,h))

  # Creating plot
  bp = ax.boxplot(data, notch=True, showfliers=False, medianprops=dict(color='red', linewidth=1.5))

  ax.set_xticks(ax.get_xticks().tolist())
  ax.set_xticklabels(tracenames)

  medians = [np.median(d) for d in data]
  for i, median in enumerate(medians):
      plt.text(i+1, median, str(f'{median:.2f}'), ha='center', va='bottom', fontsize=10)

      
  # changing style of fliers
  for flier in bp['fliers']:
      flier.set(color='#cbcbcb', alpha = 0.25)

  ax.yaxis.grid(linestyle='--')
  ax.set_ylabel(ylabel)
  ax.set_xlabel("Scheduling Algorithms")
  
  if save:
    plt.savefig(f'./figures/{ylabel}.png', dpi=300, bbox_inches='tight')
