import time
import logging

from mlinspect.inspections._inspection_input import OperatorType
from freamon.extraction.dag_extraction import find_dag_node_by_type

def extract_train_feature_matrix(dag_node_to_lineage_df):
    return _extract(OperatorType.TRAIN_DATA, dag_node_to_lineage_df)


def extract_train_labels(dag_node_to_lineage_df):
    return _extract(OperatorType.TRAIN_LABELS, dag_node_to_lineage_df)


def extract_test_feature_matrix(dag_node_to_lineage_df):
    return _extract(OperatorType.TEST_DATA, dag_node_to_lineage_df)


def extract_test_labels(dag_node_to_lineage_df):
    return _extract(OperatorType.TEST_LABELS, dag_node_to_lineage_df)


def extract_predicted_labels(dag_node_to_lineage_df):
    return _extract(OperatorType.SCORE, dag_node_to_lineage_df)


def _unpack(wrapped_rows):
    return [row.ravel() for row in wrapped_rows]


def _extract(operator_type, dag_node_to_lineage_df):

    dag_search_start = time.time()
    data_op = find_dag_node_by_type(operator_type, dag_node_to_lineage_df.keys())
    dag_search_duration = time.time() - dag_search_start
    logging.info(f'---RUNTIME: Artifact extraction > Feature matrix extraction > dag search took {dag_search_duration * 1000} ms')

    matrix_copy_start = time.time()
    matrix = _unpack(dag_node_to_lineage_df[data_op]['array'].values)
    matrix_copy_duration = time.time() - matrix_copy_start
    logging.info(f'---RUNTIME: Artifact extraction > Feature matrix extraction > matrix copy took {matrix_copy_duration * 1000} ms')

    lineage_start = time.time()
    lineage = list(dag_node_to_lineage_df[data_op]['mlinspect_lineage'])
    lineage_duration = time.time() - lineage_start
    logging.info(f'---RUNTIME: Artifact extraction > Feature matrix extraction > lineage copy took {lineage_duration * 1000} ms')


    return matrix, lineage
