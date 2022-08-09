#!/bin/bash

# PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python is required on our Azure machine due to some dependency issues

PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python run-overhead-bench.py overhead_bench_dspipes.py
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python run-overhead-bench.py overhead_bench_credit.py
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python run-overhead-bench.py overhead_bench_openml.py
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python run-overhead-bench.py overhead_bench_reviews.py
