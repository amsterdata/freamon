import logging


class DuckDBViewGenerator:

    def __init__(self, db, train_source_id_to_columns, test_source_id_to_columns):
        self.db = db
        self.train_source_id_to_columns = train_source_id_to_columns
        self.test_source_id_to_columns = test_source_id_to_columns

    def query(self, query):
        return self.db.execute(query).df()


    def test_view(self, sliceable_by, with_features, with_y_true, with_y_pred):

        columns_to_select = []
        sources_to_join = set()

        for slice_column in sliceable_by:
            for source_id, columns in self.test_source_id_to_columns.items():
                if slice_column in columns:
                    columns_to_select.append(f"fs{source_id}.{slice_column}")
                    sources_to_join.add(source_id)
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
                f"JOIN _freamon_test_source_{source_index}_with_prov_view fs{source_index} " + \
                f" ON fs{source_index}.prov_id_source_{source_index} = ftv.prov_id_source_{source_index}\n"

        query = f"""
SELECT {', '.join(columns_to_select)}
FROM _freamon_test_view ftv
{source_joins}   
        """

        logging.info(query)

        return self.db.execute(query).df()
