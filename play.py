import logging
import warnings

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

warnings.filterwarnings("ignore")

from freamon import Freamon
from freamon.compliance import FairnessMetrics, RecordUsage, DataValuation


cmd_args = []#'adult', 'num_pipe_2', 'tree']#'44', '17322']
pipeline, runtimes = Freamon().pipeline_from_py_file('pipelines--mlinspect--product-images.py', cmd_args=cmd_args)
#pipeline.compute(FairnessMetrics('third_party', 'N'))
pipeline.compute(RecordUsage())
#pipeline.compute(DataValuation())

for step, runtime in runtimes.items():
    print(step, runtime)
