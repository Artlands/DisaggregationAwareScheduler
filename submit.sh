#!/bin/bash
#SBATCH -N 1
#SBATCH -C cpu
#SBATCH -q regular
#SBATCH -J DissaggreagatedMemoryScheduling
#SBATCH --mail-user=jie.li@ttu.edu
#SBATCH --mail-type=ALL
#SBATCH -t 00:120:00

#run the application:
srun -n 20 -c 12 --cpu_bind=cores /pscratch/sd/a/artlands/DisaggregationAwareScheduler/run_bf.sh