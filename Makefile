# Run simulation
# python run_simulation.py --cluster_config fcfs_no_remote.yaml --job_config job_configs_7days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config random_no_remote.yaml --job_config job_configs_7days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config rack_no_remote.yaml --job_config job_configs_7days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config fcfs_remote.yaml --job_config job_configs_7days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config random_remote.yaml --job_config job_configs_7days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config rack_remote.yaml --job_config job_configs_7days.csv > /dev/null 2>&1 &
run:
	python run_simulation.py --cluster_config fcfs_remote.yaml --job_config job_configs_7days.csv > /dev/null 2>&1 &
	python run_simulation.py --cluster_config random_remote.yaml --job_config job_configs_7days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config rack_remote.yaml --job_config job_configs_7days.csv > /dev/null 2>&1 &

# python run_simulation.py --cluster_config rack_remote_C256_M512.yaml --job_config job_configs_7days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config rack_remote_C256_M768.yaml --job_config job_configs_7days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config rack_remote_C256_M1024.yaml --job_config job_configs_7days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config rack_remote_C256_M1280.yaml --job_config job_configs_7days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config rack_remote_C256_M1536.yaml --job_config job_configs_7days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config rack_remote_C256_M1792.yaml --job_config job_configs_7days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config rack_remote_C256_M2048.yaml --job_config job_configs_7days.csv > /dev/null 2>&1 &
# Clean monitoring files
clean:
	rm ./monitoring/*.json