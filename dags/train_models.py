import pandas as pd
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from joblib import dump

model_dict = {
    'LinearRegression' : LinearRegression(),
    'DecisionTreeRegressor' : DecisionTreeRegressor(),
    'RandomForestRegressor' : RandomForestRegressor()
}

path_to_data='/app/clean_data/fulldata.csv'
path_to_model='/app/clean_data/model.pckl'

def compute_model_score(model, X, y):
    # computing cross val
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    model_score = cross_validation.mean()

    return model_score

def prepare_data():
    # reading data
    df = pd.read_csv(path_to_data)
    # ordering data according to city and date
    df = df.sort_values(['city', 'date'], ascending=True)

    dfs = []

    for c in df['city'].unique():
        df_temp = df[df['city'] == c]

        # creating target
        df_temp.loc[:, 'target'] = df_temp['temperature'].shift(1)

        # creating features
        for i in range(1, 10):
            df_temp.loc[:, 'temp_m-{}'.format(i)
                        ] = df_temp['temperature'].shift(-i)

        # deleting null values
        df_temp = df_temp.dropna()

        dfs.append(df_temp)

    # concatenating datasets
    df_final = pd.concat(
        dfs,
        axis=0,
        ignore_index=False
    )

    # deleting date variable
    df_final = df_final.drop(['date'], axis=1)

    # creating dummies for city variable
    df_final = pd.get_dummies(df_final)

    features = df_final.drop(['target'], axis=1)
    target = df_final['target']

    return features, target

def train_model(task_instance, model_name):
    X, y = prepare_data()
    score = compute_model_score(model_dict[model_name], X, y)
    task_instance.xcom_push(
        key=model_name,
        value=score
    )

def train_best_model(task_instance, task_names):
    X, y = prepare_data()

    #Find the best model
    best_model_perf = -1000
    for model_name in model_dict.keys():
        model_perf = task_instance.xcom_pull(
                                key=model_name, 
                                task_ids=[task_names[model_name]]
                                )[0]

        if model_perf > best_model_perf:
            best_model, best_model_perf = model_dict[model_name], model_perf

    best_model.fit(X, y)
    print(str(best_model), 'saved at ', path_to_model)
    dump(best_model, path_to_model)

    
