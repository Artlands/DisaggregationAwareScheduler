#!/bin/bash

# Define the run_sim function
run_sim() {
    # Run simulation with each configuration file
    for file in $PWD/configs/cluster/gen/*; do
        if [[ $file == *.yaml ]]; then
            echo "Running simulation with configuration file: $file"
            python $PWD/run_simulation.py --cluster_config $file > /dev/null 2>&1 &
        fi
    done
}

# Call the run_sim function
run_sim

