import sys
import time

from freamon import Freamon
from freamon.templates import Output, SourceType


frm = Freamon('freamon-benchmarks-fairness', './mlruns')

captured_pipeline = None

if len(sys.argv) < 4:
    print("pipeline_file sensitive_attribute non_protected_class")

pipeline_file = sys.argv[1]
sensitive_attribute = sys.argv[2]
non_protected_class = sys.argv[3]


cmd_args = []
if len(sys.argv) > 4:
    cmd_args = sys.argv[4:]

with frm.pipeline_from_py_file(pipeline_file, cmd_args=cmd_args) as pipeline:
    captured_pipeline = pipeline


def compute_naive(pipeline, sensitive_attribute, non_protected_class):
    fact_table_index, fact_table_source = [
        (index, test_source) for index, test_source in enumerate(pipeline.test_sources)
        if test_source.source_type == SourceType.ENTITIES][0]

    fact_table_lineage = pipeline.test_source_lineage[fact_table_index]

    y_pred = pipeline.outputs[Output.Y_PRED]
    lineage_y_pred = pipeline.output_lineage[Output.Y_PRED]

    # Compute the confusion matrix per group
    y_test = pipeline.outputs[Output.Y_TEST]

    non_protected_false_negatives = 0
    non_protected_true_positives = 0
    non_protected_true_negatives = 0
    non_protected_false_positives = 0

    protected_false_negatives = 0
    protected_true_positives = 0
    protected_true_negatives = 0
    protected_false_positives = 0

    for tuple_index, annotation in enumerate(fact_table_lineage):
        tuple_annotation = list(annotation)[0]
        for index, polynomial in enumerate(lineage_y_pred):
            result = 1.0
            for variable in polynomial:
                if variable.operator_id == tuple_annotation.operator_id \
                        and variable.row_id == tuple_annotation.row_id:
                    result *= 0.0
                else:
                    result *= 1.0
            if result == 0.0:

                row = fact_table_source.data.iloc[tuple_index]

                is_non_protected = \
                    row[sensitive_attribute] == non_protected_class

                if y_test[index] == 1.0:
                    if is_non_protected:
                        if y_pred[index] == 1.0:
                            non_protected_true_positives += 1
                        else:
                            non_protected_false_negatives += 1
                    else:
                        if y_pred[index] == 1.0:
                            protected_true_positives += 1
                        else:
                            protected_false_negatives += 1
                # Negative ground truth label
                else:
                    if is_non_protected:
                        if y_pred[index] == 1.0:
                            non_protected_false_positives += 1
                        else:
                            non_protected_true_negatives += 1
                    else:
                        if y_pred[index] == 1.0:
                            protected_false_positives += 1
                        else:
                            protected_true_negatives += 1

                break

    return non_protected_true_negatives, non_protected_false_positives, non_protected_false_negatives, \
           non_protected_true_positives, protected_true_negatives, protected_false_positives, \
           protected_false_negatives, protected_true_positives


def compute_opt(pipeline, sensitive_attribute, non_protected_class):
    fact_table_index, fact_table_source = [
        (index, test_source) for index, test_source in enumerate(pipeline.test_sources)
        if test_source.source_type == SourceType.ENTITIES][0]

    fact_table_lineage = pipeline.test_source_lineage[fact_table_index]

    is_in_non_protected = list(fact_table_source.data[sensitive_attribute] == non_protected_class)

    y_pred = pipeline.outputs[Output.Y_PRED]
    lineage_y_pred = pipeline.output_lineage[Output.Y_PRED]

    # Compute the confusion matrix per group
    y_test = pipeline.outputs[Output.Y_TEST]

    non_protected_false_negatives = 0
    non_protected_true_positives = 0
    non_protected_true_negatives = 0
    non_protected_false_positives = 0

    protected_false_negatives = 0
    protected_true_positives = 0
    protected_true_negatives = 0
    protected_false_positives = 0

    for index, polynomial in enumerate(lineage_y_pred):
        for entry in polynomial:
            if entry.operator_id == fact_table_source.operator_id:
                # Positive ground truth label
                if y_test[index] == 1.0:
                    if is_in_non_protected[entry.row_id]:
                        # if is_in_non_protected_by_row_id[entry.row_id]:
                        if y_pred[index] == 1.0:
                            non_protected_true_positives += 1
                        else:
                            non_protected_false_negatives += 1
                    else:
                        if y_pred[index] == 1.0:
                            protected_true_positives += 1
                        else:
                            protected_false_negatives += 1
                # Negative ground truth label
                else:
                    if is_in_non_protected[entry.row_id]:
                        # if is_in_non_protected_by_row_id[entry.row_id]:
                        if y_pred[index] == 1.0:
                            non_protected_false_positives += 1
                        else:
                            non_protected_true_negatives += 1
                    else:
                        if y_pred[index] == 1.0:
                            protected_false_positives += 1
                        else:
                            protected_true_negatives += 1

    return non_protected_true_negatives, non_protected_false_positives, non_protected_false_negatives, \
           non_protected_true_positives, protected_true_negatives, protected_false_positives, \
           protected_false_negatives, protected_true_positives


num_repetitions = 7

for repetition in range(num_repetitions):
    print(f'# fairness on {pipeline_file} with args [{sys.argv[2:]}] -- repetition {repetition + 1} of {num_repetitions}')

    naive_start = time.time()
    naive = compute_naive(captured_pipeline, sensitive_attribute, non_protected_class)
    naive_duration = time.time() - naive_start

    print(f'fairness,naive,{naive_duration * 1000},{"|".join(sys.argv)}')

    opt_start = time.time()
    opt = compute_opt(captured_pipeline, sensitive_attribute, non_protected_class)
    opt_duration = time.time() - opt_start

    print(f'fairness,opt,{opt_duration * 1000},{"|".join(sys.argv)}')

