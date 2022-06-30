from pyspark.ml import Pipeline
from freamon.provenance.pyspark.store import SingletonProvStore


class TracedPipeline:

    def __init__(self, stages):
        self.pipeline = Pipeline(stages=stages)


    def fit(self, traced_rdd):
        SingletonProvStore().train_provenance = traced_rdd.rdd.map(lambda row_and_polynomial: row_and_polynomial[1])
        data = traced_rdd.rdd.map(lambda row_and_polynomial: row_and_polynomial[0]).toDF()
        fitted_pipeline = self.pipeline.fit(data)

        return TracedFittedPipeline(fitted_pipeline)


class TracedFittedPipeline:

    def __init__(self, fitted_pipeline):
        self.fitted_pipeline = fitted_pipeline


    def transform(self, traced_rdd):
        data = traced_rdd.rdd.map(lambda row_and_polynomial: row_and_polynomial[0]).toDF()
        predictions = self.fitted_pipeline.transform(data)

        SingletonProvStore().test_provenance = traced_rdd.rdd.map(lambda row_and_polynomial: row_and_polynomial[1])
        SingletonProvStore().predictions = predictions.select(['features', 'label', 'probability', 'prediction'])

        return predictions
