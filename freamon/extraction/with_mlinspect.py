import os
import tempfile
from contextlib import redirect_stdout
import logging
import time

from mlinspect import PipelineInspector
from mlinspect.inspections._lineage import RowLineage
from mlinspect.inspections._inspection_input import OperatorType

from freamon.templates.classification import ClassificationPipeline
from freamon.extraction import feature_matrix_extractor, source_extractor
from freamon.templates import Output
from freamon.extraction._deserialise_provenance import to_polynomials


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
                                                          OperatorType.TEST_LABELS, OperatorType.SCORE,
                                                          OperatorType.JOIN])

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

    provenance_conversion_start = time.time()
    dag_node_to_provenance = {
        node: to_polynomials(node_result['mlinspect_lineage'])
        for node, node_result in dag_node_to_intermediates.items() if node_result is not None
    }
    provenance_conversion_duration = time.time() - provenance_conversion_start
    runtimes['provenance_conversion'] = provenance_conversion_duration * 1000

    pipeline = _from_dag_and_lineage(result.dag, dag_node_to_intermediates, dag_node_to_provenance, runtimes)

    return pipeline, runtimes



def _from_dag_and_lineage(dag, dag_node_to_intermediates, dag_node_to_provenance, runtimes):
    artifact_extraction_start = time.time()

    logging.info(f'Identifying training sources')
    train_source_identification_start = time.time()
    train_sources, train_source_lineage = \
        source_extractor.extract_train_sources(dag, dag_node_to_intermediates, dag_node_to_provenance)
    train_source_identification_duration = time.time() - train_source_identification_start
    logging.info(f'---RUNTIME: Artifact extraction > Train source extraction took {train_source_identification_duration * 1000} ms')
    runtimes['train_source_extraction'] = train_source_identification_duration * 1000

    logging.info(f'Identifying test sources')
    test_source_identification_start = time.time()
    test_sources, test_source_lineage = \
        source_extractor.extract_test_sources(dag, dag_node_to_intermediates, dag_node_to_provenance)
    test_source_identification_duration = time.time() - test_source_identification_start
    logging.info(f'---RUNTIME: Artifact extraction > Test source extraction took {test_source_identification_duration * 1000} ms')
    runtimes['test_source_extraction'] = test_source_identification_duration * 1000

    feature_matrix_extraction_start = time.time()
    X_train, lineage_X_train = \
        feature_matrix_extractor.extract_train_feature_matrix(dag_node_to_intermediates, dag_node_to_provenance)
    logging.info(f'Extracted feature matrix X_train with {len(X_train)} rows and {len(X_train[0])} columns')
    X_test, lineage_X_test = \
        feature_matrix_extractor.extract_test_feature_matrix(dag_node_to_intermediates, dag_node_to_provenance)
    logging.info(f'Extracted feature matrix X_test with {len(X_test)} rows and {len(X_test[0])} columns')
    feature_matrix_extraction_duration = time.time() - feature_matrix_extraction_start
    logging.info(f'---RUNTIME: Artifact extraction > Feature matrix extraction took {feature_matrix_extraction_duration * 1000} ms')
    runtimes['feature_matrix_extraction'] = feature_matrix_extraction_duration * 1000

    label_prediction_extraction_start = time.time()
    y_train, lineage_y_train = \
        feature_matrix_extractor.extract_train_labels(dag_node_to_intermediates, dag_node_to_provenance)
    y_test, lineage_y_test = \
        feature_matrix_extractor.extract_test_labels(dag_node_to_intermediates, dag_node_to_provenance)
    y_pred, lineage_y_pred = \
        feature_matrix_extractor.extract_predicted_labels(dag_node_to_intermediates, dag_node_to_provenance)
    label_prediction_extraction_duration = time.time() - label_prediction_extraction_start
    logging.info(f'---RUNTIME: Artifact extraction > Label and prediction extraction took {label_prediction_extraction_duration * 1000} ms')
    runtimes['label_and_prediction_extraction'] = label_prediction_extraction_duration * 1000

    logging.info(f'Extracted y_train, y_test and y_pred')

    outputs = {
        Output.X_TRAIN: X_train,
        Output.Y_TRAIN: y_train,
        Output.X_TEST: X_test,
        Output.Y_TEST: y_test,
        Output.Y_PRED: y_pred
    }    

    output_lineage = {
        Output.X_TRAIN: lineage_X_train,
        Output.Y_TRAIN: lineage_y_train,
        Output.X_TEST: lineage_X_test,
        Output.Y_TEST: lineage_y_test,
        Output.Y_PRED: lineage_y_pred
    }    

    artifact_extraction_duration = time.time() - artifact_extraction_start
    logging.info(f'---RUNTIME: Artifact extraction took {artifact_extraction_duration * 1000} ms')

    return ClassificationPipeline(train_sources, train_source_lineage, test_sources, test_source_lineage,
                                  outputs, output_lineage)
