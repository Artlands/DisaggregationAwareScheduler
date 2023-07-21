import matplotlib.pyplot as plt
from matplotlib.figure import figaspect



def bar_plot(all_dfs, warmup_threshold, tracenames, metric, ylabel):
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