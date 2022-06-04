# Taken from https://github.com/DS3Lab/datascope-pipelines/

# pandas/sklearn core
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
import pandas as pd
from sklearn.model_selection import GridSearchCV

# Models
from sklearn.tree import DecisionTreeClassifier
from sklearn.preprocessing import StandardScaler, FunctionTransformer, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split

# Utils
import numpy as np
import sys


def get_clf(k):
    if k > 1:
        params = {
            "criterion": ["gini", "entropy"],
            "splitter": ["best", "random"],
            "min_samples_leaf": [1, 5]
        }

        return GridSearchCV(estimator=DecisionTreeClassifier(), param_grid=params, cv=k)
    else:
        return DecisionTreeClassifier()


def get_pipe_ops():
    #elif mode == 'num_pipe_2':
    # 2-step function scaler (*map)
    def log1p(x):
        return np.nan_to_num(np.log1p(x))

    ops = [('log', FunctionTransformer(log1p)), ('scaler', StandardScaler())]

    return ops


def create_tabular_pipeline(k, categorical_ix=[0], numerical_ix=[1, 2, 3], imputer=True):

    ops = get_pipe_ops()
    cat_pipe = Pipeline([('i', SimpleImputer(strategy='constant', fill_value='missing')),
                         ('encoder', OneHotEncoder(handle_unknown='ignore', sparse=False))])
    if imputer:
        ops = [('i', SimpleImputer(strategy='median'))] + ops
    num_pipe = Pipeline(ops)
    t = [('cat', cat_pipe, categorical_ix), ('num', num_pipe, numerical_ix)]
    col_transform = ColumnTransformer(transformers=t)
    ops = [("col_t", col_transform)]

    clf = get_clf(k)
    pipe = Pipeline(ops + [('classifier', clf)])
    return pipe


def get_dataset():
    #elif name == 'cardio':

    numerical_columns = ['age', 'height', 'weight', 'ap_hi', 'ap_lo']
    categorical_columns = ['gender', 'cholesterol', 'gluc', 'smoke', 'alco', 'active']

    data = pd.read_csv('datasets/cardio/cardio.csv', sep=';', engine='python')

    for categorical_column in categorical_columns:
        data[categorical_column] = data[categorical_column].astype(str)

    train, test = train_test_split(data, test_size=0.20, random_state=42)

    train_labels = np.array(train['cardio'])
    test_labels = np.array(test['cardio'])

    return train, train_labels, test, test_labels, numerical_columns, categorical_columns


# Make sure this code is not executed during imports
if sys.argv[0] == 'eyes' or __name__ == "__main__":

    k = 1
    if len(sys.argv) > 1:
        k = int(sys.argv[1])

    train, train_labels, test, test_labels, numerical_columns, categorical_columns = get_dataset()

    pipeline = create_tabular_pipeline(k, categorical_ix=categorical_columns, numerical_ix=numerical_columns)

    model = pipeline.fit(train, train_labels)

    print(model.score(test, test_labels))
