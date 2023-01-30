# A benchmarking tool to run parallel range reads to Bolt using the AWS Python SDK

from multiprocessing import Pool, cpu_count, Process
from subprocess import run, DEVNULL
import time
from functools import partial
from argparse import ArgumentParser
import bolt
import boto3
from shutil import rmtree
import _thread
import json
import pickle
import sys
import random 
from os.path import exists

parser = ArgumentParser('bm')
parser.add_argument('--access_key_id', type=str, default=None, help='AWS access key id ')
parser.add_argument('--secret_access_key', type=str, default=None, help='AWS secret access key')
parser.add_argument('--s3', type=bool, default=False, help='Run the benchmark directly agaisnt S3')
parser.add_argument('--bucket', type=str, default=None, help='Bucket to benchmark')
parser.add_argument('--procs', type=int, default=cpu_count(), help='number of workload procs (default instance CPU count)')
parser.add_argument('--threads', type=int, default='1', help='number of load generating threads per proc (default 1)')
parser.add_argument('--range_reads', type=bool, default=False, help='Use range reads (default False)')
parser.add_argument('--range', type=int, default='1048576', help='range to read in bytes (default 1MB)')
parser.add_argument('--random', type=bool, default=False, help='read the range at random offsets (default False)')
parser.add_argument('--run_time', type=int, default='60', help='workload run time')
flags = parser.parse_args()

def get_obj_pyclient(client, key, range):
    if flags.range_reads:
        return client.get_object(Bucket=flags.bucket,Key=key,Range=range)
    else: 
	    return client.get_object(Bucket=flags.bucket,Key=key)

# Get a range for every read request (starting at a random offset if the random flag is enabled)
def get_range(obj_size):
    read_size = flags.range
    if not flags.random:
        range = 'bytes=0-{}'.format(read_size)
    else:
        rand_int = random.randint(0, obj_size-read_size-1)	 
        range = 'bytes={}-{}'.format(rand_int, rand_int+read_size-1)
    return range

def get_key(keys, total_keys):
    return keys[random.randint(0, total_keys-1)]

# Run each thread indefinitely
#
# Retry once of exception, print response time of failure also
def run_thread_sec(keys, iter_time, boltac, obj_size):
    total_keys = len(keys)
    while True:
        range = get_range(obj_size) 
        key = get_key(keys, total_keys)
        try:
            req_start_time = time.time()
            response = get_obj_pyclient(boltac, key, range)
            text = response['Body'].read()
            print("Success: Response time: {} sec, Read Request Size: {} bytes".format((time.time()-req_start_time), len(text)))
            text = 0
        except Exception as e:
            print(e)
            print("Failure: Response time: {} sec, Read Request Size: {} bytes".format((time.time()-req_start_time), len(text)))
            print("Retrying..")
            req_start_time = time.time()
            response = get_obj_pyclient(boltac, key, range)
            text = response['Body'].read()
            print("Retry: Response time: {} sec, Read Request Size: {} bytes".format((time.time()-req_start_time), len(text)))
            print("Retry success")
            continue

# Start all threads of a process
#
# Each threads get a new bolt client and gets it's own temporary credentials 
def run_threads(keys, iter_time, thread_count, obj_size):
    for i in range(thread_count):
        retry = 10
        for i in range(retry):
            try:
                if flags.s3:
                    boltac = boto3.client('s3', aws_access_key_id=flags.access_key_id, aws_secret_access_key=flags.secret_access_key)
                else:
                    boltac = bolt.client('s3')
                _thread.start_new_thread(run_thread_sec, (keys, iter_time, boltac, obj_size ) )
                break
            except:
                print("Warn: unable to start thread, retry count {}".format(i+1))
                continue
    end_time=time.time()+iter_time
    # Run this process and it's threads until run time has passed 
    while 1:
        if time.time() > end_time:
            break
        pass

if __name__ == '__main__':
    print("""Bucket name: {} Procs: {} Threads/proc: {} Iteration Time: {}"""
            .format(flags.bucket, flags.procs, flags.threads, flags.run_time))

    tasks = []
    all_processes = []
    response2 = []
    max_processes = flags.procs
    num_threads = flags.threads
    iter_time = flags.run_time
    access_key_id=flags.access_key_id
    secret_access_key=flags.secret_access_key

    # fetch list of keys to in the Bucket
    boto_new = boto3.client('sts', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key) 
    bolt_client = bolt.client('s3')
    paginator = bolt_client.get_paginator('list_objects_v2') 
    pages = paginator.paginate(Bucket=flags.bucket)

    for page in pages:
        response2.extend(page['Contents'])

    if len(response2) == 0:
        print("No content in list response")
        exit(1)

    keys = list(map(lambda c: c["Key"], response2))
    obj_sizes = list(map(lambda c: c["Size"], response2))
    print("Total number of Keys: {}".format(len(keys)))

    # Run this task with max times
    for i in range(0, max_processes):
        tasks.append(run_threads)

    #for func in tasks:
    for ctx, func in enumerate(tasks):
        p = Process(target = func, args=(keys,iter_time,num_threads,obj_sizes[0], ))
        all_processes.append(p)
        p.start()

    for p in all_processes:
        p.join()

    print("All threads finished. End of the workload")
