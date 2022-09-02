import logging

from mlinspect.inspections._inspection_input import OperatorType


def generate_base_views(db, dag, node_to_intermediates):

    train_data_node = _find_first_by_type(dag, OperatorType.TRAIN_DATA)
    train_sources = _find_source_datasets(dag, node_to_intermediates, train_data_node.node_id)

    test_data_node = _find_first_by_type(dag, OperatorType.TEST_DATA)
    test_sources = _find_source_datasets(dag, node_to_intermediates, test_data_node.node_id)

    sources = train_sources | test_sources

    for source_id, source in sources.items():
        logging.info(f"Registering source {source_id} with columns: {list(source.columns)}")
        db.register(f'_freamon_source_{source_id}', source)

        view_creation_query = f"""
                  CREATE OR REPLACE VIEW _freamon_source_{source_id}_with_prov_view AS 
                  SELECT 
                  {_rename_columns(list(source.columns))}
                  FROM _freamon_source_{source_id}
                """

        logging.info(view_creation_query)
        db.execute(view_creation_query)

    # Register X_train with y_true
    x_train = node_to_intermediates[_find_first_by_type(dag, OperatorType.TRAIN_DATA)]
    y_train = node_to_intermediates[_find_first_by_type(dag, OperatorType.TRAIN_LABELS)]

    # TODO can we do this without copies?
    x_train.rename(columns={'array': 'features'}, inplace=True)
    x_train['y_true'] = y_train['array']

    db.register(f'_freamon_x_train', x_train)

    # Create a view with foreign keys over the test data
    # TODO Current implementation cannot handle cases where the same table is joined twice
    db.execute(f"""
    CREATE OR REPLACE VIEW _freamon_train_view AS 
        SELECT
        {_rename_columns(list(x_train.columns))}   
        FROM _freamon_x_train    
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
    # TODO Current implementation cannot handle cases where the same table is joined twice
    db.execute(f"""
    CREATE OR REPLACE VIEW _freamon_test_view AS 
        SELECT
        {_rename_columns(list(x_test.columns))}   
        FROM _freamon_x_test    
    """)

    return {source_id: list(source.columns) for source_id, source in sources.items()}


def _rename_columns(columns):
    view_columns = []
    for column in columns:
        if not column.startswith('mlinspect_lineage'):
            view_columns.append((column, column))
        else:
            source_id = column.split('_')[2]
            view_columns.append((column, f'prov_id_source_{source_id}'))
    return ', '.join([f'"{orig_name}" AS "{new_name}"' for orig_name, new_name in view_columns])


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
