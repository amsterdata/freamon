from abc import ABC, abstractmethod


class ViewGenerator(ABC):

    def __init__(self, source_id_to_columns):
        self.source_id_to_columns = source_id_to_columns

    @abstractmethod
    def execute_query(self, query):
        pass

    @abstractmethod
    def _generate_virtual_views(self):
        pass


    def test_view(self, sliceable_by, with_features, with_y_true, with_y_pred):
        columns_to_select = []
        sources_to_join = set()

        for slice_column in sliceable_by:
            for source_id, columns in self.source_id_to_columns.items():
                if slice_column in columns:
                    columns_to_select.append(f"fs{source_id}.{slice_column}")
                    sources_to_join.add(source_id)
                    break

        if with_features:
            columns_to_select.append('ft.features')

        if with_y_true:
            columns_to_select.append('ft.y_true')

        if with_y_pred:
            columns_to_select.append('ft.y_pred')

        source_joins = ''
        for source_index in sources_to_join:
            source_joins += \
                f"JOIN _freamon_source_{source_index}_with_prov fs{source_index} " + \
                f" ON fs{source_index}.prov_id_source_{source_index} = ft.prov_id_source_{source_index}\n"

        query = f"""
        SELECT {', '.join(columns_to_select)}
        FROM _freamon_test ft
        {source_joins}   
                """

        return self.execute_query(query)
