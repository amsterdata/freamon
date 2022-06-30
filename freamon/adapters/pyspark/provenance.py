from freamon.adapters.pyspark.rdd import TracedRDD
from freamon.adapters.pyspark.store import SingletonProvStore
from freamon.adapters.pyspark.pipeline import TracedPipeline


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
        df = spark.read \
            .option("header", "true") \
            .csv(path) \
            .cache()

        rdd_with_provenance = df.rdd \
            .zipWithUniqueId() \
            .map(lambda row_and_id: (row_and_id[0], {(self.source_counter, row_and_id[1])}))

        SingletonProvStore().sources[self.source_counter] = rdd_with_provenance

        self.source_counter += 1

        return TracedRDD(rdd_with_provenance)


    @staticmethod
    def make_pipeline(stages):
        return TracedPipeline(stages)

