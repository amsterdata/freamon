## freamon

__Freamon__ enables data scientists to automatically reconstruct and query the intermediate data from ML pipelines to reduce the level of expertise and manual effort required to debug this data. 

This repository contains a prototypical implementation for our abstract on _"Reconstructing and Querying ML Pipeline Intermediates"_ to be presented at [CIDR'23](https://www.cidrdb.org/cidr2023/index.html).

We provide notebooks to showcase provenance-based data debugging for a complex ML pipeline that learns to classify product reviews:

 * The [sklearn/pandas example notebook](example-sklearn-pandas.ipynb) shows how debug the data of the [ML pipeline](classify_amazonreviews_sklearn.py) implemented with sklearn and pandas.
  * The [pyspark/sparkml example notebook](example-sparkml.ipynb) shows how debug the data of the [ML pipeline](classify_amazonreviews_sparkml.py) implemented with pyspark and sparkml.


