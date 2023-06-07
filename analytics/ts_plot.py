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

def ts_plot(all_dfs, metric, algorithms):
  ylabel = metric.replace('_', ' ').title()
  labels = []
  for n in algorithms:
    if n == 'rack_disa':
      lname = 'rack_aware(rack-scale)'
    elif n == 'rack_disa_large':
      lname = 'rack_aware(rack-scale, large memorys)'
    else:
      lname = n
    labels.append(lname)

  w,h = figaspect(2/3)
  fig, ax = plt.subplots(figsize=(w,h))

  colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b','#e377c2', '#7f7f7f', '#bcbd22', '#17becf', '#1f78b4', '#33a02c']

  for i, df in enumerate(all_dfs):
    ax.plot(df['timestamp'].to_numpy(), df[f'cluster_state.{metric}'].to_numpy(), label=labels[i], color=colors[i])

  ax.yaxis.grid(linestyle='--')

  ax.set_ylabel(f"{ylabel}")
  ax.set_xlabel("Time")
  ax.legend(fontsize=10, frameon=False)
  