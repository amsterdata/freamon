import pandas as pd
import numpy as np
import sys

from sklearn.linear_model import SGDClassifier
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, FunctionTransformer, OneHotEncoder, label_binarize
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import GridSearchCV


def load_train_and_test_data(adult_train_location, adult_test_location):

    columns = ['age', 'workclass', 'fnlwgt', 'education', 'education-num', 'marital-status', 'occupation',
               'relationship', 'race', 'sex', 'capital-gain', 'capital-loss', 'hours-per-week',
               'native-country', 'income-per-year']

    adult_train = pd.read_csv(adult_train_location, names=columns, sep=', ', engine='python', na_values="?")
    adult_test = pd.read_csv(adult_test_location, names=columns, sep=', ', engine='python', na_values="?", skiprows=1)

    return adult_train, adult_test


def filter_data(train, test, excluded_employment_types):
    for employment_type in excluded_employment_types:
        train = train[train['workclass'] != employment_type]
        test = test[test['workclass'] != employment_type]

    return train, test


def create_feature_encoding_pipeline():

    def safe_log(x):
        return np.log(x, out=np.zeros_like(x), where=(x != 0))

    impute_and_one_hot_encode = Pipeline([
        ('impute', SimpleImputer(strategy='most_frequent')),
        ('encode', OneHotEncoder(sparse=False, handle_unknown='ignore'))
    ])

    impute_and_scale = Pipeline([
        ('impute', SimpleImputer(strategy='mean')),
        ('log_transform', FunctionTransformer(lambda x: safe_log(x))),
        ('scale', StandardScaler())
    ])

    featurisation = ColumnTransformer(transformers=[
        ("impute_and_one_hot_encode", impute_and_one_hot_encode, ['workclass', 'education', 'occupation']),
        ('impute_and_scale', impute_and_scale, ['age', 'capital-gain', 'capital-loss', 'hours-per-week']),
    ], remainder='drop')

    return featurisation


def extract_labels(adult_train, adult_test):
    adult_train_labels = label_binarize(adult_train['income-per-year'], classes=['<=50K', '>50K']).ravel()
    # The test data has a dot in the class names for some reason...
    adult_test_labels = label_binarize(adult_test['income-per-year'], classes=['<=50K.', '>50K.']).ravel()

    return adult_train_labels, adult_test_labels


def create_training_pipeline(k):
    featurisation = create_feature_encoding_pipeline()

    if k > 1:
        params = {
            'penalty': ['l2', 'l1'],
            'alpha': [0.0001, 0.001, 0.01, 0.1]
        }

        return Pipeline([
            ('features', featurisation),
            ('learner', GridSearchCV(estimator=SGDClassifier(loss='log'), param_grid=params, cv=k))
        ])
    else:
        return Pipeline([
            ('features', featurisation),
            ('learner', SGDClassifier(loss='log'))
        ])


def random_subset(arr):
    size = np.random.randint(low=1, high=len(arr)+1)
    choice = np.random.choice(arr, size=size, replace=False)
    return [str(item) for item in choice]


def run_pipeline(k):

    np.random.seed(42)

    train_location = 'datasets/income/adult.data'
    test_location = 'datasets/income/adult.test'

    train_all, test_all = load_train_and_test_data(train_location, test_location)

    train_all['id'] = list(range(len(train_all)))
    test_all['id'] = list(range(len(test_all)))

    workclasses_to_exclude = random_subset(['Self-emp-not-inc', 'Private', 'Local-gov', 'Self-emp-inc',
                                            'Without-pay', 'Never-worked'])

    train, test = filter_data(train_all, test_all, workclasses_to_exclude)

    train_labels, test_labels = extract_labels(train, test)
    pipeline = create_training_pipeline(k)

    model = pipeline.fit(train, train_labels)

    score = model.score(test, test_labels)

    #print("Model accuracy on held-out data", score)


# Make sure this code is not executed during imports
if sys.argv[0] == 'eyes' or __name__ == "__main__":

    if len(sys.argv) > 1:
        k = int(sys.argv[1])
        run_pipeline(k)
    else:
        print("Parameter k is missing")
        sys.exit(-1)
