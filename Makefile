# Run simulation
# python run_simulation.py --cluster_config sjf_remote.yaml --job_config job_configs_1day_all.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config fcfs_remote.yaml --job_config job_configs_1day_all.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config backfill_remote.yaml --job_config job_configs_1day_all.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config sjf_no_remote.yaml --job_config job_configs_1day_all.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config fcfs_no_remote.yaml --job_config job_configs_1day_all.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config backfill_no_remote.yaml --job_config job_configs_1day_all.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config rack_no_remote.yaml --job_config job_configs_1day_all.csv > /dev/null 2>&1 &

run:
	python run_simulation.py --cluster_config rack_disa_remote_large.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config sjf_remote.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config fcfs_remote.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config backfill_remote.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config sjf_no_remote.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config fcfs_no_remote.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config backfill_no_remote.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &
# python run_simulation.py --cluster_config rack_no_remote.yaml --job_config job_configs_3days.csv > /dev/null 2>&1 &


# Clean monitoring files
clean:
	rm ./monitoring/*.json