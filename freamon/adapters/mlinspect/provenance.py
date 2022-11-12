import os
import logging
import duckdb

from mlinspect import PipelineInspector
from mlinspect.inspections._lineage import RowLineage
from mlinspect.inspections._inspection_input import OperatorType

from freamon.adapters.mlinspect.base_views import generate_base_tables
from freamon.viewgen.duckdb import DuckDBViewGenerator


def from_sklearn_pandas(path_to_py_file, cmd_args=[]):
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

    result = inspector \
        .add_required_inspection(lineage_inspection) \
        .execute()

    dag_node_to_intermediates = {
        node: node_results[lineage_inspection]
        for node, node_results in result.dag_node_to_inspection_results.items()
    }

    db = duckdb.connect(database=':memory:')

    source_id_to_columns = generate_base_tables(db, result.dag, dag_node_to_intermediates)

    return DuckDBViewGenerator(db, source_id_to_columns)
