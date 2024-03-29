from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer, StringIndexer, OneHotEncoder, StandardScaler, VectorAssembler
from pyspark.mllib.evaluation import MulticlassMetrics
from freamon.adapters.pyspark.provenance import trace_provenance


# Data access
def load_data(tr, spark):
    reviews = tr.read_csv(spark, "datasets/reviews/reviews.csv.gz")
    products = tr.read_csv(spark, "datasets/reviews/products.csv")
    categories = tr.read_csv(spark, "datasets/reviews/categories.csv")
    ratings = tr.read_csv(spark, "datasets/reviews/ratings.csv")

    return reviews, ratings, products, categories


# Relational preprocessing
def integrate_data(reviews, ratings, products, categories, start_date):
    reviews = reviews.filter(lambda row: row['review_date'] is not None and row['review_date'] > start_date)

    reviews_with_ratings = reviews.join(ratings, left_on='review_id', right_on='review_id')
    products_with_categories = products.join(categories, left_on='category_id', right_on='id')

    reviews_with_products_and_ratings = \
        reviews_with_ratings.join(products_with_categories, left_on='product_id', right_on='product_id')

    return reviews_with_products_and_ratings


# Relational preprocessing, train/test split
def compute_feature_and_label_data(integrated_data, split_date):
    def as_str(row, key):
        if key not in row:
            return ''
        else:
            return str(row[key])

    def combine_text_cols(row):
        return as_str(row, 'product_title') + ' ' + as_str(row, 'review_headline') + ' ' + \
               as_str(row, 'review_body')

    is_helpful = lambda row: int(int(row['helpful_votes']) > 0)

    integrated_data = integrated_data.withColumn('text', combine_text_cols)
    integrated_data = integrated_data.withColumn('label', is_helpful)
    integrated_data = integrated_data.withColumn('label', is_helpful)
    integrated_data = integrated_data.withColumn('total_votes', lambda row: int(row['total_votes']))
    integrated_data = integrated_data.withColumn('star_rating', lambda row: int(row['star_rating']))

    train = integrated_data.filter(lambda row: row['review_date'] is not None and row['review_date'] <= split_date)
    test = integrated_data.filter(lambda row: row['review_date'] is not None and row['review_date'] > split_date)

    return train, test


# Feature encoding
def encode_features(categorical_columns, numerical_columns):

    stages = []

    # One-hot encode categorical features
    for categorical_column in categorical_columns:
        stages.append(StringIndexer(inputCol=categorical_column, outputCol=f"{categorical_column}Index",
                                    handleInvalid='keep'))

    categorical_indexes = [f'{categorical_column}Index' for categorical_column in categorical_columns]
    categorical_features = [f'{categorical_column}Vec' for categorical_column in categorical_columns]
    encoder = OneHotEncoder(inputCols=categorical_indexes, outputCols=categorical_features)
    numerical_assembler = VectorAssembler(inputCols=numerical_columns, outputCol="numerical_features_raw")

    stages.append(encoder)
    stages.append(numerical_assembler)

    # Normalize numerical features
    scaler = StandardScaler(inputCol='numerical_features_raw', outputCol='numerical_features', withMean=True)
    stages.append(scaler)

    # Hash word features
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    hashing = HashingTF(inputCol="words", numFeatures=100, outputCol="text_features")

    stages.append(tokenizer)
    stages.append(hashing)

    # Concatenate all features
    assembler = VectorAssembler(inputCols=categorical_features + ['numerical_features', 'text_features'],
        outputCol="features")

    stages.append(assembler)

    return stages


# Learner definition
def add_learner(stages):
    # Train a logistic regression model
    stages.append(LogisticRegression(maxIter=10, regParam=0.001))
    return stages


def run_pipeline(spark, start_date, split_date):
    with trace_provenance() as tr:
        categorical_columns = ['category']
        numerical_columns = ['total_votes', 'star_rating']

        # Relational preprocessing
        reviews, ratings, products, categories = load_data(tr, spark)
        integrated_data = integrate_data(reviews, ratings, products, categories, start_date)
        train, test = compute_feature_and_label_data(integrated_data, split_date)

        # Feature encoding & learning
        stages = encode_features(categorical_columns, numerical_columns)
        stages = add_learner(stages)

        # Training and evaluation
        model = tr.make_pipeline(stages=stages).fit(train)
        predictions = model.transform(test)

        predictionsAndLabels = predictions.select(['prediction', 'label']).rdd \
            .map(lambda row: (row['prediction'], float(row['label'])))

        metrics = MulticlassMetrics(predictionsAndLabels)
        print(f'Accuracy: {metrics.accuracy}')
