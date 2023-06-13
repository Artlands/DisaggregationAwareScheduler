#!/bin/bash

# Define the run_bf function
run_bf() {
    # Run simulation with each configuration file ending in "_bf.yaml"
    for file in ./configs/cluster/gen/*; do
        if [[ $file == *.yaml ]]; then
            # Run simulation with each configuration file
            echo "Running simulation with configuration file: $file"
            python run_simulation.py --cluster_config $file --job_config job_configs_3days.csv > /dev/null 2>&1 &
        fi
    done
}

# Call the run_bf function
run_bf
