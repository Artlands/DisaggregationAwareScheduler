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
colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b','#e377c2', '#7f7f7f', '#bcbd22', '#17becf', '#1f78b4', '#33a02c']

def ts_plot(all_dfs, metric, tracenames):
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
  

def line_comp_plot(lists, names, xticks):  
  w,h = figaspect(2/3)
  fig, ax0 = plt.subplots(figsize=(w,h))
  ax1 = ax0.twinx()

  ax0.plot(xticks, lists[0], label=names[0], color=colors[0])
  ax1.plot(xticks, lists[1], label=names[1], color=colors[1])
  
  ax0.yaxis.grid(linestyle='--')
  
  ax0.set_ylabel(names[0])
  ax1.set_ylabel(names[1])

  ax0.set_xlabel("Memory Node Capacity (TB) per Rack")
  # ax0.legend(frameon=False, fontsize=10)
  # ax1.legend(frameon=False, fontsize=10)
  lines0, labels0 = ax0.get_legend_handles_labels()
  lines1, labels1 = ax1.get_legend_handles_labels()
  ax1.legend(lines1 + lines0, labels1 + labels0, frameon=False, fontsize=10)
  

def line_single_plot(lists, names, xticks):  
  w,h = figaspect(2/3)
  fig, ax0 = plt.subplots(figsize=(w,h))

  ax0.plot(xticks, lists[0], label=names[0], color=colors[0])
  
  ax0.yaxis.grid(linestyle='--')
  
  ax0.set_ylabel(names[0])

  ax0.set_xlabel("Memory Node Capacity (TB) per Rack")
  ax0.legend(frameon=False, fontsize=10)
  