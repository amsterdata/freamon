#!/bin/bash

# PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python is required on our Azure machine due to some dependency issues

PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python analysis--fairness.py pipelines--openml--multiple.py V3 b\'married\' 1461 17315
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python analysis--fairness.py pipelines--openml--multiple.py V3 b\'married\' 1461 17326
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python analysis--fairness.py pipelines--openml--multiple.py V3 b\'married\' 1461 17322
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python analysis--fairness.py pipelines--openml--multiple.py V3 b\'married\' 1461 8774

PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python analysis--fairness.py pipelines--dspipes--multiple.py race White adult num_pipe_0 logistic
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python analysis--fairness.py pipelines--dspipes--multiple.py race White adult num_pipe_1 logistic
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python analysis--fairness.py pipelines--dspipes--multiple.py race White adult num_pipe_2 logistic
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python analysis--fairness.py pipelines--dspipes--multiple.py race White adult num_pipe_0 tree
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python analysis--fairness.py pipelines--dspipes--multiple.py race White adult num_pipe_1 tree
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python analysis--fairness.py pipelines--dspipes--multiple.py race White adult num_pipe_2 tree

PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python analysis--fairness.py pipelines--dspipes--multiple.py sex Male adult num_pipe_0 logistic
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python analysis--fairness.py pipelines--dspipes--multiple.py sex Male adult num_pipe_1 logistic
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python analysis--fairness.py pipelines--dspipes--multiple.py sex Male adult num_pipe_2 logistic
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python analysis--fairness.py pipelines--dspipes--multiple.py sex Male adult num_pipe_0 tree
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python analysis--fairness.py pipelines--dspipes--multiple.py sex Male adult num_pipe_1 tree
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python analysis--fairness.py pipelines--dspipes--multiple.py sex Male adult num_pipe_2 tree

