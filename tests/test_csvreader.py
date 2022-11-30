from utils.csv_reader import CSVReader

def test_csvreader():
  jobs_csv = './job_configs.csv'
  csv_reader = CSVReader(jobs_csv)
  job_configs = csv_reader.generate(0, 14110)
  assert len(job_configs) == 14110