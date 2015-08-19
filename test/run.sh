#!/bin/bash 

rm -rf ./data 

mkdir -p ./data/data_sampler 
mkdir -p ./data/table_checker
mkdir -p ./data/table_join_checker 
mkdir -p ./data/data_sampler_output

python make_data.py 
python test_data_sampler_task.py
