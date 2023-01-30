# Install Bolt SDK

This SDK provides an authentication solution for programatically interacting with Bolt. It wraps the boto3 interface so project wide integration is as easy as refactoring `import boto3` to `import bolt as boto3`.

The package affects the signing and routing protocol of the boto3 S3 client, therefore any non S3 clients created through this SDK will be un-affected by the wrapper.

## Prerequisites

The minimum supported version of Python is version 3.

## Installation

```bash
python3 -m pip install bolt-sdk
```

# Expose Bolt Custom Domain

Declare the ENV variable: `BOLT_CUSTOM_DOMAIN`, which constructs Bolt URL and hostname based on default naming
```bash
export BOLT_CUSTOM_DOMAIN="example.com"
```

# Run the Benchmarks

1. Fill in user inputs in the `run_all_workloads.sh`

2. Execute the benchmarks
```bash
./run_all_workloads.sh 
```
