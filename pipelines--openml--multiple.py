import numpy as np
import pandas as pd
import sys
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import RobustScaler
from sklearn.svm import SVC
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder, FunctionTransformer
from sklearn.linear_model import LogisticRegression


def get_flow(openml_id, numerical_columns, categorical_columns):
    flow = None
    applicable_on_dataframe = False

    if openml_id == '5055':
        flow = Pipeline([('scale', RobustScaler()), ('rf', RandomForestClassifier())])

    elif openml_id == '8774':
        num_pipe = Pipeline([('imputer', SimpleImputer(add_indicator=True)),
                             ('standardscaler', StandardScaler())])
        cat_pipe = Pipeline([('simpleimputer', SimpleImputer(strategy='most_frequent')),
                             ('onehotencoder', OneHotEncoder())])
        applicable_on_dataframe = True
        flow = Pipeline([
            ('columntransformer', ColumnTransformer([
                ('num', num_pipe, numerical_columns),
                ('cat', cat_pipe, categorical_columns),
            ])),
            ('decisiontreeclassifier', DecisionTreeClassifier())])

    elif openml_id == '17315':
        num_pipe = Pipeline([('simpleimputer', SimpleImputer()), ('standardscaler', StandardScaler())])
        applicable_on_dataframe = True
        flow = Pipeline([
            ('columntransformer', ColumnTransformer([('cont', num_pipe, numerical_columns)])),
            ('decisiontreeclassifier', DecisionTreeClassifier())])

    elif openml_id == '17326':
        applicable_on_dataframe = True
        flow = Pipeline([
            ('columntransformer', ColumnTransformer([
                ('num', Pipeline([('standardscaler', StandardScaler())]), numerical_columns),
                ('cat', Pipeline([('onehotencoder', OneHotEncoder())]), categorical_columns),
            ])),
            ('logisticregression', LogisticRegression(solver='liblinear'))])

    elif openml_id == '17322':

        def another_imputer(df_with_categorical_columns):
            return df_with_categorical_columns.fillna('__missing__')

        applicable_on_dataframe = True
        num_pipe = Pipeline([('imputer', SimpleImputer()),
                             ('standardscaler', StandardScaler())])
        cat_pipe = Pipeline([('anothersimpleimputer', FunctionTransformer(another_imputer)),
                             ('onehotencoder', OneHotEncoder())])
        applicable_on_dataframe = True
        flow = Pipeline([
            ('columntransformer', ColumnTransformer([
                ('num', num_pipe, numerical_columns),
                ('cat', cat_pipe, categorical_columns),
            ])),
            ('decisiontreeclassifier', DecisionTreeClassifier())])

    elif openml_id == '17337':
        num_pipe = Pipeline([('simpleimputer', SimpleImputer()), ('standardscaler', StandardScaler())])
        cat_pipe = Pipeline([('simpleimputer', SimpleImputer(strategy='most_frequent')),
                             ('onehotencoder', OneHotEncoder())])
        applicable_on_dataframe = True
        flow = Pipeline([
            ('columntransformer', ColumnTransformer([('num', num_pipe, numerical_columns),
                                                     ('cat', cat_pipe, categorical_columns)])),
            ('svc', SVC())])

    elif openml_id == '17655' or openml_id == '18576':
        num_pipe = Pipeline([('simpleimputer', SimpleImputer()), ('standardscaler', StandardScaler())])
        cat_pipe = Pipeline([('simpleimputer', SimpleImputer(strategy='most_frequent')),
                             ('onehotencoder', OneHotEncoder())])
        applicable_on_dataframe = True
        flow = Pipeline([
            ('columntransformer', ColumnTransformer([('num', num_pipe, numerical_columns),
                                                     ('cat', cat_pipe, categorical_columns)])),
            ('randomforestclassifier', RandomForestClassifier())])


    elif openml_id == '17355':
        flow = Pipeline([('imputer', SimpleImputer()), ('classifier', SVC())])

    elif openml_id == '17400':
        flow = Pipeline([('standardscaler', SimpleImputer()), ('svc', SVC())])

    elif openml_id == '17496':
        flow = Pipeline([('simpleimputer', SimpleImputer()), ('decisiontreeclassifier', DecisionTreeClassifier())])

    elif openml_id == '18922':
        flow = Pipeline([('imputer', SimpleImputer()), ('estimator', DecisionTreeClassifier())])

    elif openml_id == '18720':
        applicable_on_dataframe = True
        flow = Pipeline([
            ('columntransformer', ColumnTransformer([('num', StandardScaler(), numerical_columns),
                                                     ('cat', OneHotEncoder(), categorical_columns)])),
            ('svc', SVC())])
    else:
        raise ValueError(f"Invalid flow id: {openml_id}!")

    return flow, applicable_on_dataframe


def get_dataset(openml_id, seed):

    numerical_columns = []
    categorical_columns = []
    train = None
    train_labels = None
    test = None
    test_labels = None

    if openml_id == '44':
        # Spambase dataset (44)
        data = pd.read_csv('datasets/spambase/dataset_44_spambase.csv')

        label_column = 'class'
        numerical_columns = [column for column in data.columns if column != label_column]

        data_train, data_test = train_test_split(data, test_size=0.2, random_state=seed)

        train = data_train[numerical_columns]
        train_labels = np.array(data_train[label_column] == "b'1'")

        test = data_test[numerical_columns]
        test_labels = np.array(data_test[label_column] == "b'1'")

    elif openml_id == '246':
        # BNG labor dataset (246)
        data = pd.read_csv('datasets/labor/dataset_246_labor.csv.gz', compression='gzip')
        label_column = 'class'
        numerical_columns = ['duration', 'wage-increase-first-year', 'wage-increase-second-year',
                             'wage-increase-third-year', 'working-hours', 'standby-pay',
                             'shift-differential', 'statutory-holidays']
        categorical_columns = ['cost-of-living-adjustment', 'pension', 'education-allowance', 'vacation',
                               'longterm-disability-assistance', 'contribution-to-dental-plan',
                               'bereavement-assistance', 'contribution-to-health-plan']

        data_train, data_test = train_test_split(data, test_size=0.2, random_state=seed)

        train = data_train[numerical_columns + categorical_columns]
        train_labels = np.array(data_train[label_column] == "b'good'")

        test = data_test[numerical_columns + categorical_columns]
        test_labels = np.array(data_test[label_column] == "b'good'")

    elif openml_id == '823':
        # Houses dataset (823)
        data = pd.read_csv('datasets/houses/dataset_823_houses.csv')
        label_column = 'binaryClass'
        numerical_columns = ['median_house_value', 'median_income', 'housing_median_age',
                             'total_rooms', 'total_bedrooms', 'population', 'households', 'latitude']
        categorical_columns = []

        data_train, data_test = train_test_split(data, test_size=0.2, random_state=seed)

        train = data_train[numerical_columns + categorical_columns]
        train_labels = np.array(data_train[label_column] == "b'P'")

        test = data_test[numerical_columns + categorical_columns]
        test_labels = np.array(data_test[label_column] == "b'P'")

    elif openml_id == '1461':
        # Bankmarketing dataset (44)
        data = pd.read_csv('datasets/bankmarketing/dataset_1461_bankmarketing.csv')
        label_column = 'Class'
        numerical_columns = ['V1', 'V6', 'V10', 'V12', 'V13', 'V14', 'V15']
        categorical_columns = ['V2', 'V3', 'V4', 'V5', 'V7', 'V8', 'V9', 'V11', 'V16']

        data_train, data_test = train_test_split(data, test_size=0.2, random_state=seed)

        train = data_train[numerical_columns + categorical_columns]
        train_labels = np.array(data_train[label_column] == "b'2'")

        test = data_test[numerical_columns + categorical_columns]
        test_labels = np.array(data_test[label_column] == "b'2'")

    else:
        raise ValueError("Invalid dataset id!")

    return train, train_labels, test, test_labels, numerical_columns, categorical_columns


# Make sure this code is not executed during imports
if sys.argv[0] == 'eyes' or __name__ == "__main__":

    dataset_id = '1461'
    if len(sys.argv) > 1:
        dataset_id = sys.argv[1]

    flow_id = '17326'
    if len(sys.argv) > 2:
        flow_id = sys.argv[2]

    seed = 1234
    np.random.seed(seed)

    print(f'Running flow {flow_id} on dataset {dataset_id}')
    train, train_labels, test, test_labels, numerical_columns, categorical_columns = get_dataset(dataset_id, seed)

    flow, applicable_on_df = get_flow(flow_id, numerical_columns, categorical_columns)

    if not applicable_on_df:
        model = flow.fit(train[numerical_columns].values, train_labels)
        print('    Score: ', model.score(test[numerical_columns].values, test_labels))
        pass
    else:
        model = flow.fit(train, train_labels)
        print('    Score: ', model.score(test, test_labels))
