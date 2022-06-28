import os
import tempfile
from contextlib import redirect_stdout
import logging
import time

from mlinspect import PipelineInspector
from mlinspect.inspections._lineage import RowLineage
from mlinspect.inspections._inspection_input import OperatorType

from freamon.viewgen.duckdb import DuckDBViewGenerator


def from_py_file(path_to_py_file, cmd_args=[]):
    synthetic_cmd_args = ['eyes']
    synthetic_cmd_args.extend(cmd_args)
    from unittest.mock import patch
    import sys
    logging.info(f'Patching sys.argv with {synthetic_cmd_args}')
    with patch.object(sys, 'argv', synthetic_cmd_args):
        return _execute_pipeline(PipelineInspector.on_pipeline_from_py_file(path_to_py_file))


def _execute_pipeline(inspector: PipelineInspector):
    lineage_inspection = RowLineage(RowLineage.ALL_ROWS, [OperatorType.DATA_SOURCE, OperatorType.TRAIN_DATA,
                                                          OperatorType.TRAIN_LABELS, OperatorType.TEST_DATA,
                                                          OperatorType.TEST_LABELS, OperatorType.SCORE])

    os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

    runtimes = {}

    logging.info('Executing instrumented user pipeline with mlinspect')
    with tempfile.TemporaryDirectory() as tmpdirname:
        logging.info('Redirecting the pipeline\'s stdout to pipeline-output.txt')
        with open(os.path.join(tmpdirname, 'pipeline-output.txt'), 'w') as tmpfile:
            with redirect_stdout(tmpfile):
                mlinspect_start = time.time()
                result = inspector \
                    .add_required_inspection(lineage_inspection) \
                    .execute()
                mlinspect_duration = time.time() - mlinspect_start

    logging.info(f'---RUNTIME: Instrumented execution took {mlinspect_duration * 1000} ms')
    runtimes['instrumentation'] = mlinspect_duration * 1000

    dag_node_to_intermediates = {
        node: node_results[lineage_inspection]
        for node, node_results in result.dag_node_to_inspection_results.items()
    }

    return DuckDBViewGenerator(result.dag, dag_node_to_intermediates), runtimes
    # return result.dag, dag_node_to_intermediates, runtimes
