import pandas as pd
from operator import attrgetter

from core.job import JobConfig


class CSVReader(object):
  def __init__(self, filename):
    self.filename = filename
    df = pd.read_csv(self.filename)

    job_configs = []
    for i in range(len(df)):
      series = df.iloc[i]
      submit = series.submit
      nnodes = series.nnodes
      memory = series.memory
      duration = series.duration

      job_configs.append(JobConfig(submit, nnodes, memory, duration))

    job_configs.sort(key=attrgetter('submit'))

    self.job_configs = job_configs

  def generate(self, offset, number):
    number = number if offset + number < len(self.job_configs) else len(self.job_configs) - offset
    ret = self.job_configs[offset: offset + number]
    the_first_job_config = ret[0]
    submit_base = the_first_job_config.submit

    for job_config in ret:
      job_config.submit -= submit_base

    print(f'Jobs number: {len(ret)}')

    return ret