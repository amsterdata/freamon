import logging

from freamon.viewgen.view_gen import ViewGenerator


class DuckDBViewGenerator(ViewGenerator):

    def __init__(self, db, source_id_to_columns):
        self.db = db
        super(DuckDBViewGenerator, self).__init__(source_id_to_columns)

        self._generate_virtual_views()

    def _generate_virtual_views(self):

        for train_or_test in ['train', 'test']:
            base_columns = list(self.db.execute(f'DESCRIBE _freamon_{train_or_test}').df().column_name)

            joins = ''
            columns_to_exclude = []

            for source_id in self.source_id_to_columns:
                if f'prov_id_source_{source_id}' in base_columns:
                    joins += f'  JOIN _freamon_source_{source_id}_with_prov s{source_id} ON\n'
                    joins += f'    s{source_id}.prov_id_source_{source_id} = t.prov_id_source_{source_id}\n'
                    columns_to_exclude.append(f'prov_id_source_{source_id}')

            query = \
                f'''
CREATE OR REPLACE VIEW _freamon_virtual_{train_or_test}_view AS 
  SELECT * EXCLUDE ({", ".join(columns_to_exclude)})
  FROM _freamon_{train_or_test} t
  {joins}
            '''

            logging.info(f'Generating virtual {train_or_test} view via \n\n{query}')
            self.db.execute(query)


    def execute_query(self, query):
        return self.db.execute(query).df()

