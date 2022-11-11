from pyspark.sql import Row
import pyspark.sql.functions as func
import logging


def add_provenance_columns(row_and_id):
    row = row_and_id[0]
    row_dict = row.asDict()

    polynomial = list(row_and_id[1])
    # Required for toDF to produce correct results...
    polynomial.sort(key=lambda tup: tup[0])

    for element in polynomial:
        source_id = element[0]
        row_id = element[1]
        row_dict[f'prov_id_source_{source_id}'] = row_id

    return Row(**row_dict)


def generate_base_views(prov_store, db):
    source_id_to_column = {}

    for source_id, source in prov_store.sources.items():
        source_with_provenance = source.map(add_provenance_columns).toDF().toPandas()

        logging.info(f"Registering source {source_id} with columns: {list(source_with_provenance.columns)}")
        db.register(f'_freamon_source_{source_id}_with_prov_view', source_with_provenance)
        source_id_to_column[source_id] = source_with_provenance.columns

    logging.info(f"Computing test features and predictions...")
    test_features_and_predictions = prov_store.test_features_and_predictions
    test_provenance = prov_store.test_provenance

    test_data_with_provenance_rdd = test_features_and_predictions.rdd.zip(test_provenance).map(add_provenance_columns)
    test_data_with_provenance = test_data_with_provenance_rdd.toDF() \
        .withColumnRenamed('label', 'y_true') \
        .withColumn("y_pred", func.round(func.col("prediction"), 0)) \
        .drop("prediction") \
        .toPandas()

    test_data_with_provenance['features'] = test_data_with_provenance['features'].transform(lambda v: v.toArray())

    db.register(f'_freamon_test_view', test_data_with_provenance)

    logging.info(f"Computing train features and predictions...")
    train_features = prov_store.train_features
    train_provenance = prov_store.train_provenance

    train_data_with_provenance_rdd = train_features.rdd.zip(train_provenance).map(add_provenance_columns)
    train_data_with_provenance = train_data_with_provenance_rdd.toDF().toPandas()
    train_data_with_provenance['features'] = train_data_with_provenance['features'].transform(lambda v: v.toArray())

    db.register(f'_freamon_train_view', train_data_with_provenance)

    return source_id_to_column
