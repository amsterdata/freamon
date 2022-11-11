import duckdb

from freamon.adapters.pyspark.rdd import TracedRDD
from freamon.adapters.pyspark.store import SingletonProvStore
from freamon.adapters.pyspark.pipeline import TracedPipeline
from freamon.adapters.pyspark.base_views import generate_base_views
from freamon.viewgen.duckdb import DuckDBViewGenerator


def from_trace():
    db = duckdb.connect(database=':memory:')
    return DuckDBViewGenerator(db, generate_base_views(SingletonProvStore(), db))


def trace_provenance():
    return ProvenanceTracing()


class ProvenanceTracing:

    def __init__(self):
        self.source_counter = 0

    def __enter__(self):
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def read_csv(self, spark, path):

        source_id = self.source_counter

        df = spark.read \
            .option("header", "true") \
            .csv(path) \
            .cache()

        rdd_with_provenance = df.rdd \
            .zipWithUniqueId() \
            .map(lambda row_and_id: (row_and_id[0], {(source_id, row_and_id[1])}))

        SingletonProvStore().sources[source_id] = rdd_with_provenance

        self.source_counter += 1

        return TracedRDD(rdd_with_provenance)


    @staticmethod
    def make_pipeline(stages):
        return TracedPipeline(stages)

