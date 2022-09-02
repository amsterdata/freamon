from pyspark.sql import Row
import pyspark.sql.functions as func

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



def generate_base_views(prov_store):

    source_id_to_column = {}

    for source_id, source in prov_store.sources.items():
        source_with_provenance = source.map(add_provenance_columns).toDF()
        source_with_provenance.createOrReplaceTempView(f'_freamon_source_{source_id}_with_prov_view')
        source_id_to_column[source_id] = source_with_provenance.columns

    test_features_and_predictions = prov_store.test_features_and_predictions
    test_provenance = prov_store.test_provenance

    test_data_with_provenance = test_features_and_predictions.rdd.zip(test_provenance).map(add_provenance_columns)
    test_data_with_provenance.toDF()\
        .withColumnRenamed('label', 'y_true') \
        .withColumn("y_pred", func.round(func.col("prediction"), 0))\
        .drop("prediction")\
        .createOrReplaceTempView("_freamon_test_view")

    train_features = prov_store.train_features
    train_provenance = prov_store.train_provenance

    train_data_with_provenance = train_features.rdd.zip(train_provenance).map(add_provenance_columns)

    train_data_with_provenance.toDF().createOrReplaceTempView("_freamon_train_view")

    return source_id_to_column
