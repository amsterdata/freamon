import logging
import warnings

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

warnings.filterwarnings("ignore")

from freamon import Freamon
from freamon.compliance import FairnessMetrics, RecordUsage, DataValuation

frm = Freamon()

cmd_args = []#'1461', '18922']
with frm.pipeline_from_py_file('pipelines--mlinspect--credit.py', cmd_args=cmd_args) as pipeline:
    #pipeline.compute(FairnessMetrics('third_party', 'N'))
    pipeline.compute(RecordUsage())
    #res = pipeline.compute(DataValuation())

    print(f"It worked")
