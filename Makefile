.PHONY:
	all clean

# A Python script that generates a configuration file using the config_gen.py utility.
gen_config:
	python ./utils/config_gen.py 

all: run

run: gen_config
	bash run_bf.sh

# A target that removes all .json files in the ./monitoring directory
clean:
	rm ./monitoring/*.json