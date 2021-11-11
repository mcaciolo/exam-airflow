import requests
import json
import datetime
import os
import pandas as pd

from airflow.models import Variable

def write_data_to_json():
    'Get the data'
    data=[]
    for city in Variable.get(key='cities').split(","):
        r = requests.get(
            url='https://api.openweathermap.org/data/2.5/weather',
            params= {
                'q': city,
                'appid':Variable.get(key='api_key')
            }
        )
        data.append(json.loads(r.text))
    
    file_name = f'/app/raw_files/{datetime.datetime.now().strftime("%Y%m%d %H:%M")}.json'
    with open(file_name, 'w') as file:
        json.dump(data, file)


def transform_data_into_csv(n_files=None, filename='data.csv'):
    parent_folder = '/app/raw_files'
    files = sorted(os.listdir(parent_folder), reverse=True)
    if n_files:
        files = files[:n_files]

    dfs = []

    for f in files:
        with open(os.path.join(parent_folder, f), 'r') as file:
            data_temp = json.load(file)
        for data_city in data_temp:
            dfs.append(
                {
                    'temperature': data_city['main']['temp'],
                    'city': data_city['name'],
                    'pression': data_city['main']['pressure'],
                    'date': f.split('.')[0]
                }
            )

    df = pd.DataFrame(dfs)

    print('\n', df.head(10))

    df.to_csv(os.path.join('/app/clean_data', filename), index=False)

