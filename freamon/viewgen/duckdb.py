from freamon.viewgen.view_gen import ViewGenerator


class DuckDBViewGenerator(ViewGenerator):

    def __init__(self, db, source_id_to_columns):
        self.db = db
        super(DuckDBViewGenerator, self).__init__(source_id_to_columns)

    def execute_query(self, query):
        return self.db.execute(query).df()

