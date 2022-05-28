#!/bin/bash

# PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python is required on our Azure machine due to some dependency issues

PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--record-usage.py pipelines--dspipes--multiple.py adult num_pipe_0 logistic
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--record-usage.py pipelines--dspipes--multiple.py adult num_pipe_1 tree
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--record-usage.py pipelines--dspipes--multiple.py adult num_pipe_2 logistic
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--record-usage.py pipelines--dspipes--multiple.py cardio num_pipe_0 logistic
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--record-usage.py pipelines--dspipes--multiple.py cardio num_pipe_1 tree
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--record-usage.py pipelines--dspipes--multiple.py cardio num_pipe_2 logistic

PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--record-usage.py pipelines--openml--multiple.py 44 17326
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--record-usage.py pipelines--openml--multiple.py 44 17322
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--record-usage.py pipelines--openml--multiple.py 44 8774
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--record-usage.py pipelines--openml--multiple.py 1461 17326
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--record-usage.py pipelines--openml--multiple.py 1461 17322
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--record-usage.py pipelines--openml--multiple.py 1461 8774

PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--record-usage.py pipelines--mlinspect--amazon-reviews.py
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--record-usage.py pipelines--mlinspect--credit.py
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--record-usage.py pipelines--mlinspect--product-images.py



#PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--data-valuation.py pipelines--openml--multiple.py 44 17326
#PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--data-valuation.py pipelines--openml--multiple.py 44 17322

#PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--fairness.py pipelines--openml--multiple.py V3 married 1461 17326
#PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--fairness.py pipelines--openml--multiple.py V3 married 1461 17322