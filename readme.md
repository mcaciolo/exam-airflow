# AIRFLOW EXAM

### Introduction

This repository includes a containered version of airflow (provided by the DataScientest team) and the DAG file that I coded to build a sample pipeline to extract weather data, train three sample models and select the most performant one.

![Airflow pipeline](/dag-eval.png)

In order to use the pipeline, you need to create an account and obtain an api key from [OpenWeatherMap](https://openweathermap.org/). 

### Instructions to use the repository

- Create the folders logs, plugins, raw_file and clean_data : `mkdir ./logs ./plugins ./raw_files ./clean_data`
- Init the containers : docker-compose up -d
- Go to airflow dashboard ([localhost:8080](localhost:8080)) and add two variables :
    - *api-key* : your key from [OpenWeatherMap](https://openweathermap.org/)
    - *cities* : comma-separated list of cities (i.e. `paris,london,washington`)
- From the airflow dashboard, look for the weather_dag DAG (tags : `datascientest`, `weather`) and activate it
- After some minutes, you will find your model on /clean_data/model.pckl. It will be updated every minute with the last data.

### Folders

**dags** : The folder containing the airflow DAG


### Author

Marcello CACIOLO