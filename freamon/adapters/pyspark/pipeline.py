from pyspark.ml import Pipeline
from freamon.adapters.pyspark.store import SingletonProvStore


class TracedPipeline:

    def __init__(self, stages):
        encoding_stages = stages[:-1]
        estimator_stage = stages[-1]
        self.encoding_pipeline = Pipeline(stages=encoding_stages)
        self.estimator = estimator_stage


    def fit(self, traced_rdd):

        traced_rdd.cache()

        SingletonProvStore().train_provenance = traced_rdd.rdd.map(lambda row_and_polynomial: row_and_polynomial[1])
        data = traced_rdd.rdd.map(lambda row_and_polynomial: row_and_polynomial[0]).toDF()
        fitted_encoding_pipeline = self.encoding_pipeline.fit(data)

        transformed_data = fitted_encoding_pipeline.transform(data)
        SingletonProvStore().train_features = transformed_data.select(['features', 'label'])

        SingletonProvStore().train_features.cache()

        fitted_estimator = self.estimator.fit(transformed_data)

        return TracedFittedPipeline(fitted_encoding_pipeline, fitted_estimator)


class TracedFittedPipeline:

    def __init__(self, fitted_encoding_pipeline, fitted_estimator):
        self.fitted_encoding_pipeline = fitted_encoding_pipeline
        self.fitted_estimator = fitted_estimator


    def transform(self, traced_rdd):

        traced_rdd.cache()

        data = traced_rdd.rdd.map(lambda row_and_polynomial: row_and_polynomial[0]).toDF()

        encoded_data = self.fitted_encoding_pipeline.transform(data)
        predictions = self.fitted_estimator.transform(encoded_data)

        predictions.cache()

        SingletonProvStore().test_provenance = traced_rdd.rdd.map(lambda row_and_polynomial: row_and_polynomial[1])
        SingletonProvStore().test_features_and_predictions = \
            predictions.select(['features', 'label', 'probability', 'prediction'])

        return predictions
