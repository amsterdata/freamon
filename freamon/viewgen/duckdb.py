import logging
import duckdb

from mlinspect.inspections._inspection_input import OperatorType


class DuckDBViewGenerator:

    def __init__(self, dag, node_to_intermediates):
        self.dag = dag
        self.node_to_intermediates = node_to_intermediates

        self.db = duckdb.connect(database=':memory:')
        self.test_sources = {}

        self._init_test_view()

    def _init_test_view(self):

        test_data_node = self._find_first_by_type(OperatorType.TEST_DATA)
        self.test_sources = self._find_source_datasets(test_data_node.node_id)

        # Register test_sources
        for source_id, source in self.test_sources.items():

            logging.info(f"Registering test source {source_id} with columns: {list(source.columns)}")

            self.db.register(f'_freamon_source_{source_id}', source)
            self.db.execute(f"""
              CREATE OR REPLACE VIEW _freamon_source_{source_id}_with_prov_view AS 
              SELECT 
                * EXCLUDE mlinspect_lineage, 
                CAST(string_to_array(trim(mlinspect_lineage, ')('), ',')[2] AS INT) AS prov_id_source_{source_id}
              FROM _freamon_source_{source_id}
            """)

        # Register X_test with y_true and y_pred
        x_test = self.node_to_intermediates[self._find_first_by_type(OperatorType.TEST_DATA)]
        y_test = self.node_to_intermediates[self._find_first_by_type(OperatorType.TEST_LABELS)]
        y_pred = self.node_to_intermediates[self._find_first_by_type(OperatorType.SCORE)]

        # TODO can we do this without copies?
        x_test.rename(columns={'array': 'features'}, inplace=True)
        x_test['y_true'] = y_test['array']
        x_test['y_pred'] = y_pred['array']

        self.db.register(f'_freamon_x_test', x_test)

        # Create a view with foreign keys over the test data
        provenance_based_fk_columns = ''

        for position, source_index in enumerate(self.test_sources.keys()):
            provenance_based_fk_columns += \
                f"CAST(regexp_replace(polynomial[{position + 1}], '\(\\d+,|\)', '', 'g') AS INT) " + \
                f"AS prov_id_source_{source_index},\n"

        # TODO this might have some issues if the same table is joined twice, because we lose the order in list_distinct
        self.db.execute(f"""
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

    def test_view(self, sliceable_by, with_features, with_y_true, with_y_pred):

        columns_to_select = []
        sources_to_join = set()

        for slice_column in sliceable_by:
            for source_index, source in self.test_sources.items():
                if slice_column in list(source.columns):
                    columns_to_select.append(f"fs{source_index}.{slice_column}")
                    sources_to_join.add(source_index)
                    break

        if with_features:
            columns_to_select.append('ftv.features')

        if with_y_true:
            columns_to_select.append('ftv.y_true')

        if with_y_pred:
            columns_to_select.append('ftv.y_pred')

        source_joins = ''
        for source_index in sources_to_join:
            source_joins += \
                f"JOIN _freamon_source_{source_index}_with_prov_view fs{source_index} " + \
                f" ON fs{source_index}.prov_id_source_{source_index} = ftv.prov_id_source_{source_index}\n"

        query = f"""
SELECT {', '.join(columns_to_select)}
FROM _freamon_test_view ftv
{source_joins}   
        """

        logging.info(query)

        return self.db.execute(query).df()


    def _find_source_datasets(self, start_node_id):
        nodes_to_search = []
        nodes_processed = set()

        source_datasets = {}

        nodes_to_search.append(start_node_id)

        while len(nodes_to_search) > 0:
            current_node_id = nodes_to_search.pop()
            for source, target in self.dag.edges:
                if target.node_id == current_node_id:
                    if source.node_id not in nodes_processed and source.node_id not in nodes_to_search:
                        nodes_to_search.append(source.node_id)
                        if source.operator_info.operator == OperatorType.DATA_SOURCE:
                            data = self.node_to_intermediates[source]
                            source_datasets[source.node_id] = data
            nodes_processed.add(current_node_id)

        return source_datasets

    def _find_first_by_type(self, operator_type):
        for node in self.dag.nodes:
            if node.operator_info.operator == operator_type:
                return node
        raise ValueError(f'Unable to find DAG node of type {operator_type}')