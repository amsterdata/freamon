import sys
import time

from freamon import Freamon
from freamon.templates import Output


frm = Freamon()

captured_pipeline = None

pipeline_file = sys.argv[1]

cmd_args = []
if len(sys.argv) > 2:
    cmd_args = sys.argv[2:]

with frm.pipeline_from_py_file(pipeline_file, cmd_args=cmd_args) as pipeline:
    captured_pipeline = pipeline


def record_usage_naive(pipeline):
    lineage_X_train = pipeline.output_lineage[Output.X_TRAIN]

    usages_per_source = {}

    for index, source in enumerate(pipeline.train_sources):
        usages_per_source[index] = set()

        source_lineage = pipeline.train_source_lineage[index]

        for tuple_index, annotation in enumerate(source_lineage):
            tuple_annotation = list(annotation)[0]
            for polynomial in lineage_X_train:
                result = 1.0
                for variable in polynomial:
                    if variable.operator_id == tuple_annotation.operator_id \
                            and variable.row_id == tuple_annotation.row_id:
                        result *= 0.0
                    else:
                        result *= 1.0
                if result == 0.0:
                    usages_per_source[index].add(tuple_index)
                    break

    return usages_per_source


def record_usage_opt(pipeline):
    usages_per_source = {source_index: set() for source_index in range(len(pipeline.train_sources))}
    for polynomial in pipeline.output_lineage[Output.X_TRAIN]:
        for entry in polynomial:
            usages_per_source[entry.operator_id].add(entry.row_id)
    return usages_per_source



num_repetitions = 7

for repetition in range(num_repetitions):
    print(f'# record usage on {pipeline_file} with args [{sys.argv[2:]}] -- repetition {repetition + 1} of {num_repetitions}')

    naive_start = time.time()
    naive = record_usage_naive(captured_pipeline)
    naive_duration = time.time() - naive_start

    print(f'record_usage,naive,{naive_duration * 1000},{"|".join(sys.argv)}')

    opt_start = time.time()
    opt = record_usage_opt(captured_pipeline)
    opt_duration = time.time() - opt_start

    print(f'record_usage,opt,{opt_duration * 1000},{"|".join(sys.argv)}')