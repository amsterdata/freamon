import logging

from mlinspect.inspections._inspection_input import OperatorType


def generate_base_views(db, dag, node_to_intermediates):
    test_data_node = _find_first_by_type(dag, OperatorType.TEST_DATA)
    test_sources = _find_source_datasets(dag, node_to_intermediates, test_data_node.node_id)

    for source_id, source in test_sources.items():
        logging.info(f"Registering test source {source_id} with columns: {list(source.columns)}")

        db.register(f'_freamon_source_{source_id}', source)
        db.execute(f"""
                  CREATE OR REPLACE VIEW _freamon_source_{source_id}_with_prov_view AS 
                  SELECT 
                    * EXCLUDE mlinspect_lineage, 
                    CAST(string_to_array(trim(mlinspect_lineage, ')('), ',')[2] AS INT) AS prov_id_source_{source_id}
                  FROM _freamon_source_{source_id}
                """)

    # Register X_test with y_true and y_pred
    x_test = node_to_intermediates[_find_first_by_type(dag, OperatorType.TEST_DATA)]
    y_test = node_to_intermediates[_find_first_by_type(dag, OperatorType.TEST_LABELS)]
    y_pred = node_to_intermediates[_find_first_by_type(dag, OperatorType.SCORE)]

    # TODO can we do this without copies?
    x_test.rename(columns={'array': 'features'}, inplace=True)
    x_test['y_true'] = y_test['array']
    x_test['y_pred'] = y_pred['array']

    db.register(f'_freamon_x_test', x_test)

    # Create a view with foreign keys over the test data
    provenance_based_fk_columns = ''

    for position, source_index in enumerate(test_sources.keys()):
        provenance_based_fk_columns += \
            f"CAST(regexp_replace(polynomial[{position + 1}], '\(\\d+,|\)', '', 'g') AS INT) " + \
            f"AS prov_id_source_{source_index},\n"

    # TODO this might have some issues if the same table is joined twice, because we lose the order in list_distinct
    db.execute(f"""
              CREATE OR REPLACE VIEW _freamon_test_view AS 
              SELECT 
                features, y_true, y_pred,
                {provenance_based_fk_columns}    
              FROM (
                SELECT 
                  features, y_true, y_pred,
                  list_sort(list_distinct(string_to_array(mlinspect_lineage, ';'))) AS polynomial
                FROM _freamon_x_test
              )
            """)

    return {source_id: list(source.columns) for source_id, source in test_sources.items()}



def _find_source_datasets(dag, node_to_intermediates, start_node_id):
    nodes_to_search = []
    nodes_processed = set()
    source_datasets = {}

    nodes_to_search.append(start_node_id)

    while len(nodes_to_search) > 0:
        current_node_id = nodes_to_search.pop()
        for source, target in dag.edges:
            if target.node_id == current_node_id:
                if source.node_id not in nodes_processed and source.node_id not in nodes_to_search:
                    nodes_to_search.append(source.node_id)
                    if source.operator_info.operator == OperatorType.DATA_SOURCE:
                        data = node_to_intermediates[source]
                        source_datasets[source.node_id] = data
        nodes_processed.add(current_node_id)

    return source_datasets


def _find_first_by_type(dag, operator_type):
    for node in dag.nodes:
        if node.operator_info.operator == operator_type:
            return node
    raise ValueError(f'Unable to find DAG node of type {operator_type}')
