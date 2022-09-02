from freamon.viewgen.view_gen import ViewGenerator


class SparkViewGenerator(ViewGenerator):

    def __init__(self, spark, source_id_to_columns):
        self.spark = spark
        super(SparkViewGenerator, self).__init__(source_id_to_columns)

    def execute_query(self, query):
        return self.spark.sql(query).toPandas()

