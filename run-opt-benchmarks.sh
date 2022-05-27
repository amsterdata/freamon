#!/bin/bash

#python benchmarks--record-usage.py pipelines--openml--multiple.py 44 17326
#python benchmarks--record-usage.py pipelines--openml--multiple.py 44 17322

#python benchmarks--data-valuation.py pipelines--openml--multiple.py 44 17326
#python benchmarks--data-valuation.py pipelines--openml--multiple.py 44 17322

python benchmarks--fairness.py pipelines--openml--multiple.py V3 married 1461 17326
python benchmarks--fairness.py pipelines--openml--multiple.py V3 married 1461 17322