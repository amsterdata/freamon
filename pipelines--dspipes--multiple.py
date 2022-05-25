# Taken from https://github.com/DS3Lab/datascope-pipelines/

# pandas/sklearn core
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import FeatureUnion
from sklearn.decomposition import PCA, TruncatedSVD
from sklearn.preprocessing import label_binarize
import pandas as pd

# Models
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler, FunctionTransformer, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.model_selection import train_test_split

# Utils
import numpy as np
import string
import re
import sys


def get_clf(mode, **kwargs):
    '''
    Code returning sklearn classifier for pipelines
    '''
    if mode == 'logistic':
        solver = kwargs.get('solver', 'liblinear')
        n_jobs = kwargs.get('n_jobs', None)
        max_iter = kwargs.get('max_iter', 5000)
        model = LogisticRegression(solver=solver, n_jobs=n_jobs,
                                   max_iter=max_iter, random_state=666)
    elif mode == 'tree':
        model = DecisionTreeClassifier()

    else:
        raise ValueError("Invalid model!")

    return model


def get_pipe_ops(mode):
    if mode == 'num_pipe_0':
        # identity pipeline
        def identity(x):
            return x

        ops = [('identity', FunctionTransformer(identity))]

    elif mode == 'num_pipe_1':
        # 1-step scaler (*map)
        ops = [('scaler', StandardScaler())]

    elif mode == 'num_pipe_2':
        # 2-step function scaler (*map)
        def log1p(x):
            return np.nan_to_num(np.log1p(x))

        ops = [('log', FunctionTransformer(log1p)), ('scaler', StandardScaler())]

    elif mode == 'num_pipe_3':
        # multiple dimensionality reductions (fork)
        union = FeatureUnion([("pca", PCA(n_components=2)),
                             ("svd", TruncatedSVD(n_iter=1))
                            ])

        ops = [('union', union),('scaler', StandardScaler())]

    elif mode == 'txt_pipe_0':
        # text pipeline
        ops = [('vect', CountVectorizer()), ('tfidf', TfidfTransformer())]

    elif mode == 'txt_pipe_1':

        def text_lowercase(text_array):
            return list(map(lambda x: x.lower(), text_array))

        def remove_urls(text_array):
            return [str(' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", text).split()))
                    for text in text_array]

        ops = [('lower_case', FunctionTransformer(text_lowercase)),
               ('remove_url', FunctionTransformer(remove_urls)),
               ('vect', CountVectorizer()), ('tfidf', TfidfTransformer())]

    elif mode == 'txt_pipe_2':

        def remove_numbers(text_array):
            return [str(re.sub(r'\d+', '', text)) for text in text_array]

        def remove_punctuation(text_array):
            translator = str.maketrans('', '', string.punctuation)
            return [text.translate(translator) for text in text_array]

        def text_lowercase(text_array):
            return list(map(lambda x: x.lower(), text_array))

        def remove_urls(text_array):
            return [str(' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", text).split()))
                    for text in text_array]

        ops = [('lower_case', FunctionTransformer(text_lowercase)),
               ('remove_url', FunctionTransformer(remove_urls)),
               ('remove_numbers', FunctionTransformer(remove_numbers)),
               ('remove_punctuation', FunctionTransformer(remove_punctuation)),
               ('vect', CountVectorizer()), ('tfidf', TfidfTransformer())]

    else:
        raise ValueError("Invalid mode!")

    return ops


def create_tabular_pipeline(num_mode, categorical_ix=[0], numerical_ix=[1, 2, 3], imputer=True, clf_mode='logistic',
                            **kwargs):
    ops = get_pipe_ops(num_mode)
    cat_pipe = Pipeline([('i', SimpleImputer(strategy='constant', fill_value='missing')),
                         ('encoder', OneHotEncoder(handle_unknown='ignore', sparse=False))])
    if imputer:
        ops = [('i', SimpleImputer(strategy='median'))] + ops
    num_pipe = Pipeline(ops)
    t = [('cat', cat_pipe, categorical_ix), ('num', num_pipe, numerical_ix)]
    col_transform = ColumnTransformer(transformers=t)
    ops = [("col_t", col_transform)]

    clf = get_clf(clf_mode)
    pipe = Pipeline(ops + [('classifier', clf)])
    return pipe


def create_text_pipeline(txt_mode, clf_mode='logistic'):
    txt_ops = Pipeline(get_pipe_ops(txt_mode))

    features = ColumnTransformer([('txt', txt_ops, 'text')], sparse_threshold=0)
    return Pipeline([('features', features), ('classifier', get_clf(clf_mode))])


def get_dataset(name):
    if name == 'adult':
        columns = ['age', 'workclass', 'fnlwgt', 'education', 'education-num', 'marital-status', 'occupation',
                   'relationship', 'race', 'sex', 'capital-gain', 'capital-loss', 'hours-per-week',
                   'native-country', 'income-per-year']

        train_location = 'datasets/income/adult.data'
        train = pd.read_csv(train_location, names=columns, sep=', ', engine='python', na_values="?")
        train_labels = label_binarize(train['income-per-year'], classes=['<=50K', '>50K']).ravel()

        test_location = 'datasets/income/adult.test'
        test = pd.read_csv(test_location, names=columns, sep=', ', engine='python', na_values="?", skiprows=1)
        # The test data has a dot in the class names for some reason...
        test_labels = label_binarize(test['income-per-year'], classes=['<=50K.', '>50K.']).ravel()

        numerical_columns = ['age', 'capital-gain', 'capital-loss']
        categorical_columns = ['workclass', 'education', 'marital-status', 'race']

        return train, train_labels, test, test_labels, numerical_columns, categorical_columns

    elif name == 'cardio':

        numerical_columns = ['age', 'height', 'weight', 'ap_hi', 'ap_lo']
        categorical_columns = ['gender', 'cholesterol', 'gluc', 'smoke', 'alco', 'active']

        data = pd.read_csv('datasets/cardio/cardio.csv', sep=';', engine='python')

        for categorical_column in categorical_columns:
            data[categorical_column] = data[categorical_column].astype(str)

        train, test = train_test_split(data, test_size=0.20, random_state=42)

        train_labels = np.array(train['cardio'])
        test_labels = np.array(test['cardio'])

        return train, train_labels, test, test_labels, numerical_columns, categorical_columns


    elif name == 'tweets':

        data = pd.read_csv('datasets/twitter/twitter-airline-sentiment.csv', sep=',', engine='python')

        train_columns, test_columns = train_test_split(data, test_size=0.20, random_state=42)

        train = pd.DataFrame.from_dict({'text': train_columns['text']})
        test = pd.DataFrame.from_dict({'text': test_columns['text']})

        train_labels = train_columns['airline_sentiment'] == 'positive'
        test_labels = test_columns['airline_sentiment'] == 'positive'

        return train, train_labels, test, test_labels, [], []

    elif name == 'twenty':
        from sklearn.datasets import fetch_20newsgroups

        cats = ['alt.atheism', 'sci.space']
        newsgroups_train = fetch_20newsgroups(subset='train', categories=cats)
        newsgroups_test = fetch_20newsgroups(subset='test', categories=cats)

        train = pd.DataFrame.from_dict({'text': newsgroups_train.data})
        test = pd.DataFrame.from_dict({'text': newsgroups_test.data})

        return train, newsgroups_train.target, test, newsgroups_test.target, [], []
    else:
        raise ValueError("Invalid dataset name!")


# Make sure this code is not executed during imports
if sys.argv[0] == 'eyes' or __name__ == "__main__":

    chosen_dataset = 'adult'
    if len(sys.argv) > 1:
        chosen_dataset = sys.argv[1]

    chosen_type = 'num_pipe_0'
    if len(sys.argv) > 2:
        chosen_type = sys.argv[2]

    chosen_model = 'logistic'
    if len(sys.argv) > 3:
        chosen_model = sys.argv[3]

    print(f'data: {chosen_dataset}, type: {chosen_type}, clf: {chosen_model}')

    train, train_labels, test, test_labels, numerical_columns, categorical_columns = get_dataset(chosen_dataset)

    if list(train.columns) == ['text']:
        pipeline = create_text_pipeline(txt_mode=chosen_type, clf_mode=chosen_model)
    else:
        pipeline = create_tabular_pipeline(num_mode=chosen_type, categorical_ix=categorical_columns,
                                           numerical_ix=numerical_columns, clf_mode=chosen_model)

    model = pipeline.fit(train, train_labels)

    print(model.score(test, test_labels))
