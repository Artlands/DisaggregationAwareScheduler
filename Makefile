# Run simulation
run:
	python run_simulation.py --cluster_config baseline_fcfs.yaml > /dev/null 2>&1 &
	python run_simulation.py --cluster_config dis_fcfs.yaml > /dev/null 2>&1 &

# Clean monitoring files
clean:
	rm ./monitoring/*.json