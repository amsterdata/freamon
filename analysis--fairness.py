import sys
import warnings
warnings.filterwarnings("ignore")

from freamon import Freamon
from freamon.compliance import FairnessMetrics

frm = Freamon()


if len(sys.argv) < 4:
    print("pipeline_file sensitive_attribute non_protected_class")

pipeline_file = sys.argv[1]
sensitive_attribute = sys.argv[2]
non_protected_class = sys.argv[3]


cmd_args = []
if len(sys.argv) > 4:
    cmd_args = sys.argv[4:]

with frm.pipeline_from_py_file(pipeline_file, cmd_args=cmd_args) as pipeline:
    non_protected_true_negatives, non_protected_false_positives, non_protected_false_negatives, \
    non_protected_true_positives, protected_true_negatives, protected_false_positives, \
    protected_false_negatives, protected_true_positives = \
        pipeline.compute(FairnessMetrics(sensitive_attribute, non_protected_class))

    print(f'{pipeline_file},{sensitive_attribute},{non_protected_class},{"|".join(cmd_args)},{non_protected_true_negatives},' +
          f'{non_protected_false_positives},{non_protected_false_negatives},{non_protected_true_positives},' +
          f'{protected_true_negatives},{protected_false_positives},{protected_false_negatives},' +
          f'{protected_true_positives}')
