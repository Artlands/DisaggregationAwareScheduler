run:
# python run_simulation.py --cluster_config baseline.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
	python run_simulation.py --cluster_config fcfs.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
	python run_simulation.py --cluster_config fcfs_bf.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
	python run_simulation.py --cluster_config sjf.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
	python run_simulation.py --cluster_config sjf_bf.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
	python run_simulation.py --cluster_config laf.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
	python run_simulation.py --cluster_config laf_bf.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config rack_scale.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config system_scale.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &


# Clean monitoring files
clean:
	rm ./monitoring/*.json