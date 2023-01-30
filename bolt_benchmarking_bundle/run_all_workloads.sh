#!/bin/bash

# User Input 
bucket=bucket_name # Bucked that needs to be benchmarked
access_key_id='ABC' # AWS access key id
secret_access_key='XYZ' # AWS secret access key

# Workload Inputs (pre-configured)
runtime=1800 # Run time per iteration
sleeptime=180 # Sleep time between interation 
sz_iterations=(1048576 10485760 524288000) # Read request sizes 1MB, 10 MB & 500 MB 
t_iterations=(1 2 4) # Threads per workload process

echo "Starting workloads..."

for range_size in "${sz_iterations[@]}"
do	
    for threads in "${t_iterations[@]}"
    do	
    python3 perfbench_get_objs.py --access_key_id $access_key_id --secret_access_key $secret_access_key --bucket $bucket --random True --range_reads True --range $range_size --threads $threads  --run_time $runtime
    sleep $sleeptime
    done	
done
