import logging
import warnings

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

warnings.filterwarnings("ignore")

from freamon import Freamon
from freamon.compliance import FairnessMetrics, RecordUsage, DataValuation

frm = Freamon()

cmd_args = ['44', '17322']
with frm.pipeline_from_py_file('pipelines--openml--multiple.py', cmd_args=cmd_args) as pipeline:
    #pipeline.compute(FairnessMetrics('third_party', 'N'))
    pipeline.compute(RecordUsage())
    #pipeline.compute(DataValuation())

    print(f"It worked")
