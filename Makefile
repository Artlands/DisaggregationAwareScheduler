run_bf:
# python run_simulation.py --cluster_config fcfs_bf.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config sjf_bf.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config lsf_bf.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
	python run_simulation.py --cluster_config f1_bf.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config wfp3_bf.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config unicep_bf.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &

run_non_bf:
	python run_simulation.py --cluster_config fcfs.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
	python run_simulation.py --cluster_config sjf.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
	python run_simulation.py --cluster_config lsf.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
	python run_simulation.py --cluster_config f1.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
	python run_simulation.py --cluster_config wfp3.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
	python run_simulation.py --cluster_config unicep.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &

# Clean monitoring files
clean:
	rm ./monitoring/*.json