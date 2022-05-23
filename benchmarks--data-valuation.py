import sys
import time
import numpy as np
from numba import njit, prange

from freamon import Freamon
from freamon.templates import Output, SourceType


frm = Freamon('freamon-benchmarks-data-valuation', './mlruns')

captured_pipeline = None

pipeline_file = sys.argv[1]

cmd_args = []
if len(sys.argv) > 2:
    cmd_args = sys.argv[2:]

with frm.pipeline_from_py_file(pipeline_file, cmd_args=cmd_args) as pipeline:
    captured_pipeline = pipeline


@njit(fastmath=True, parallel=True)
def _compute_shapley_values(X_train, y_train, X_test, y_test, K=1):
    N = len(X_train)
    M = len(X_test)
    result = np.zeros(N, dtype=np.float32)

    for j in prange(M):
        score = np.zeros(N, dtype=np.float32)
        dist = np.zeros(N, dtype=np.float32)
        div_range = np.arange(1.0, N)
        div_min = np.minimum(div_range, K)
        for i in range(N):
            dist[i] = np.sqrt(np.sum(np.square(X_train[i] - X_test[j])))
        indices = np.argsort(dist)
        y_sorted = y_train[indices]
        eq_check = (y_sorted == y_test[j]) * 1.0
        diff = - 1 / K * (eq_check[1:] - eq_check[:-1])
        diff /= div_range
        diff *= div_min
        score[indices[:-1]] = diff
        score[indices[-1]] = eq_check[-1] / N
        score[indices] += np.sum(score[indices]) - np.cumsum(score[indices])
        result += score / M

    return result


def data_valuation_naive(pipeline, num_test_samples, k):
    X_train = pipeline.outputs[Output.X_TRAIN]
    X_test = pipeline.outputs[Output.X_TEST]
    y_train = pipeline.outputs[Output.Y_TRAIN]
    y_test = pipeline.outputs[Output.Y_TEST]

    X_test_sampled = X_test[:num_test_samples, :]
    y_test_sampled = y_test[:num_test_samples, :]

    shapley_values = _compute_shapley_values(X_train,
                                             np.squeeze(y_train),
                                             X_test_sampled,
                                             np.squeeze(y_test_sampled), k)

    lineage_X_train = pipeline.output_lineage[Output.X_TRAIN]

    fact_table_index, fact_table_source = [
        (index, train_source) for index, train_source in enumerate(pipeline.train_sources)
        if train_source.source_type == SourceType.ENTITIES][0]

    source_lineage = pipeline.train_source_lineage[fact_table_index]

    assigned_shapley_values = np.zeros(len(fact_table_source.data))

    for tuple_index, annotation in enumerate(source_lineage):
        tuple_annotation = list(annotation)[0]
        for train_index, polynomial in enumerate(lineage_X_train):
            result = 1.0
            for variable in polynomial:
                if variable.operator_id == tuple_annotation.operator_id \
                        and variable.row_id == tuple_annotation.row_id:
                    result *= 0.0
                else:
                    result *= 1.0
            if result == 0.0:
                assigned_shapley_values[tuple_index] = shapley_values[train_index]
                break

    return assigned_shapley_values


def data_valuation_opt(pipeline, num_test_samples, k):
    X_train = pipeline.outputs[Output.X_TRAIN]
    X_test = pipeline.outputs[Output.X_TEST]
    y_train = pipeline.outputs[Output.Y_TRAIN]
    y_test = pipeline.outputs[Output.Y_TEST]

    X_test_sampled = X_test[:num_test_samples, :]
    y_test_sampled = y_test[:num_test_samples, :]

    shapley_values = _compute_shapley_values(X_train,
                                             np.squeeze(y_train),
                                             X_test_sampled,
                                             np.squeeze(y_test_sampled), k)

    lineage_X_train = pipeline.output_lineage[Output.X_TRAIN]

    fact_table_index, fact_table_source = [
        (index, train_source) for index, train_source in enumerate(pipeline.train_sources)
        if train_source.source_type == SourceType.ENTITIES][0]

    fact_table_operator_id = fact_table_source.operator_id
    assigned_shapley_values = np.zeros(len(fact_table_source.data))

    for train_index, polynomial in enumerate(lineage_X_train):
        for entry in polynomial:
            if entry.operator_id == fact_table_operator_id:
                assigned_shapley_values[entry.row_id] = shapley_values[train_index]

    return assigned_shapley_values



num_repetitions = 7

for repetition in range(num_repetitions):
    print(f'# data_valuation on {pipeline_file} with args [{sys.argv[2:]}] -- repetition {repetition + 1} of {num_repetitions}')

    naive_start = time.time()
    naive = data_valuation_naive(captured_pipeline, 10, 1)
    naive_duration = time.time() - naive_start

    print(f'data_valuation,naive,{naive_duration * 1000},{"|".join(sys.argv)}')

    opt_start = time.time()
    opt = data_valuation_opt(captured_pipeline, 10, 1)
    opt_duration = time.time() - opt_start

    print(f'data_valuation,opt,{opt_duration * 1000},{"|".join(sys.argv)}')

