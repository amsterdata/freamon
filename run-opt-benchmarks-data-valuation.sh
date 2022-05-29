#!/bin/bash

# PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python is required on our Azure machine due to some dependency issues

PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--data-valuation.py pipelines--dspipes--multiple.py adult num_pipe_0 logistic
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--data-valuation.py pipelines--dspipes--multiple.py adult num_pipe_1 tree
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--data-valuation.py pipelines--dspipes--multiple.py adult num_pipe_2 logistic
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--data-valuation.py pipelines--dspipes--multiple.py cardio-sampled num_pipe_0 logistic
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--data-valuation.py pipelines--dspipes--multiple.py cardio-sampled num_pipe_1 tree
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--data-valuation.py pipelines--dspipes--multiple.py cardio-sampled num_pipe_2 logistic

PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--data-valuation.py pipelines--openml--multiple.py 44 17326
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--data-valuation.py pipelines--openml--multiple.py 44 17322
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--data-valuation.py pipelines--openml--multiple.py 44 8774
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--data-valuation.py pipelines--openml--multiple.py 1461 17326
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--data-valuation.py pipelines--openml--multiple.py 1461 17322
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--data-valuation.py pipelines--openml--multiple.py 1461 8774

PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--data-valuation.py pipelines--mlinspect--credit.py
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--data-valuation.py pipelines--mlinspect--product-images.py

