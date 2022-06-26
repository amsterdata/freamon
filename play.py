import logging
import warnings

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

warnings.filterwarnings("ignore")

from freamon import Freamon
#from freamon.compliance import FairnessMetrics, RecordUsage, DataValuation


cmd_args = []
pipeline, runtimes = Freamon().pipeline_from_py_file('pipelines--mlinspect--product-images.py', cmd_args=cmd_args)
