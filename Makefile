# Run simulation
run:
	python run_simulation.py --cluster_config fcfs_no_remote.yaml --job_config job_configs_7days.csv > /dev/null 2>&1 &
	python run_simulation.py --cluster_config fcfs_remote.yaml --job_config job_configs_7days.csv > /dev/null 2>&1 &
	python run_simulation.py --cluster_config random_no_remote.yaml --job_config job_configs_7days.csv > /dev/null 2>&1 &
	python run_simulation.py --cluster_config random_remote.yaml --job_config job_configs_7days.csv > /dev/null 2>&1 &
	python run_simulation.py --cluster_config rack_no_remote.yaml --job_config job_configs_7days.csv > /dev/null 2>&1 &
	python run_simulation.py --cluster_config rack_remote.yaml --job_config job_configs_7days.csv > /dev/null 2>&1 &

# Clean monitoring files
clean:
	rm ./monitoring/*.json