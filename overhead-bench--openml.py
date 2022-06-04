import numpy as np
import pandas as pd
import sys
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.tree import DecisionTreeClassifier

from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.model_selection import GridSearchCV


def get_flow(numerical_columns, categorical_columns, k):
    #8774
    num_pipe = Pipeline([('imputer', SimpleImputer(add_indicator=True)),
                         ('standardscaler', StandardScaler())])
    cat_pipe = Pipeline([('simpleimputer', SimpleImputer(strategy='most_frequent')),
                         ('onehotencoder', OneHotEncoder())])

    params = {
        "criterion": ["gini", "entropy"],
        "splitter": ["best", "random"],
        "min_samples_leaf": [1, 5]
    }

    applicable_on_dataframe = True

    if k > 1:
        flow = Pipeline([
            ('columntransformer', ColumnTransformer([
                ('num', num_pipe, numerical_columns),
                ('cat', cat_pipe, categorical_columns),
            ])),
            ('decisiontreeclassifier', GridSearchCV(estimator=DecisionTreeClassifier(), param_grid=params, cv=k))
        ])
    else:
        flow = Pipeline([
            ('columntransformer', ColumnTransformer([
                ('num', num_pipe, numerical_columns),
                ('cat', cat_pipe, categorical_columns),
            ])),
            ('decisiontreeclassifier', DecisionTreeClassifier())
        ])

    return flow, applicable_on_dataframe


def get_dataset(seed):

    numerical_columns = []
    categorical_columns = []
    train = None
    train_labels = None
    test = None
    test_labels = None

    #1461

    # Bankmarketing dataset (1461)
    data = pd.read_csv('datasets/bankmarketing/dataset_1461_bankmarketing.csv')
    label_column = 'Class'
    numerical_columns = ['V1', 'V6', 'V10', 'V12', 'V13', 'V14', 'V15']
    categorical_columns = ['V2', 'V3', 'V4', 'V5', 'V7', 'V8', 'V9', 'V11', 'V16']

    data_train, data_test = train_test_split(data, test_size=0.2, random_state=seed)

    train = data_train[numerical_columns + categorical_columns]
    train_labels = np.array(data_train[label_column] == "b'2'")

    test = data_test[numerical_columns + categorical_columns]
    test_labels = np.array(data_test[label_column] == "b'2'")


    return train, train_labels, test, test_labels, numerical_columns, categorical_columns


# Make sure this code is not executed during imports
if sys.argv[0] == 'eyes' or __name__ == "__main__":

    k = 1
    if len(sys.argv) > 1:
        k = int(sys.argv[1])

    seed = 1234
    np.random.seed(seed)

    train, train_labels, test, test_labels, numerical_columns, categorical_columns = get_dataset(seed)

    flow, applicable_on_df = get_flow(numerical_columns, categorical_columns, k)

    if not applicable_on_df:
        model = flow.fit(train[numerical_columns].values, train_labels)
        print('    Score: ', model.score(test[numerical_columns].values, test_labels))
        pass
    else:
        model = flow.fit(train, train_labels)
        print('    Score: ', model.score(test, test_labels))
