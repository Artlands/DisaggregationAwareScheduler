.PHONY:
	all clean
	
run:
	bash run_sim.sh

# A Python script that generates a configuration file using the config_gen.py utility.
gen:
	python ./utils/config_gen.py 

# A target that removes all .json files in the ./monitoring directory
clean:
	rm ./monitoring/*.json ./configs/cluster/gen/*.yaml