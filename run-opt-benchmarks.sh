#!/bin/bash

# Required on our Azure machine due to some dependency issues


PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--record-usage.py pipelines--openml--multiple.py 44 17326
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--record-usage.py pipelines--openml--multiple.py 44 17322

PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--data-valuation.py pipelines--openml--multiple.py 44 17326
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--data-valuation.py pipelines--openml--multiple.py 44 17322

PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--fairness.py pipelines--openml--multiple.py V3 married 1461 17326
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python benchmarks--fairness.py pipelines--openml--multiple.py V3 married 1461 17322