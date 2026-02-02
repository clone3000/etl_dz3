from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import numpy as np

def extract_temperature():

    csv_path = '/opt/airflow/data/IOT-temp.csv'
        
    df = pd.read_csv(csv_path)    
    print(f"Загружено {len(df)} записей")
    
    raw_path = '/tmp/temperature_raw.parquet'
    df.to_parquet(raw_path, index=False)
    
    return {'total_records': len(df), 'raw_path': raw_path}

def transform_temperature(**context):
    data = context['ti'].xcom_pull(task_ids='extract_temperature')
    raw_path = data['raw_path']
    
    df = pd.read_parquet(raw_path)
    print(f"{len(df)} записей")
    
    print("\n1. Фильтр: out/in = 'In'")
    df = df[df['out/in'] == 'In']
    print(f"   Осталось: {len(df)} записей")

    print("\n2. Преобразование даты")
    try:
        df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M', errors='coerce')
        df = df.dropna(subset=['noted_date'])
        df['date_only'] = df['noted_date'].dt.date
        
        print(f"   Диапазон дат: {df['date_only'].min()} - {df['date_only'].max()}")
    except Exception as e:
        print(f"   Ошибка преобразования даты: {e}")
        df['date_only'] = pd.to_datetime(df['noted_date'].str.split().str[0], format='%d-%m-%Y')
    
    print("\n3. Очистка температуры (5-й и 95-й процентиль)")
    
    lower_bound = df['temp'].quantile(0.05)
    upper_bound = df['temp'].quantile(0.95)
    
    print(f"   5-й процентиль: {lower_bound:.2f}°C")
    print(f"   95-й процентиль: {upper_bound:.2f}°C")
    
    df['temp_cleaned'] = df['temp'].clip(lower=lower_bound, upper=upper_bound)
    
    print(f"   До очистки: {df['temp'].min():.1f} - {df['temp'].max():.1f}°C")
    print(f"   После очистки: {df['temp_cleaned'].min():.1f} - {df['temp_cleaned'].max():.1f}°C")
    
    print("\n4. Поиск самых жарких/холодных дней")
    
    daily_avg = df.groupby('date_only')['temp_cleaned'].mean().reset_index()
    daily_avg.columns = ['date', 'avg_temp']
    hottest = daily_avg.nlargest(5, 'avg_temp')
    coldest = daily_avg.nsmallest(5, 'avg_temp')
    
    print("\n   5-Самых жарких дней:")
    for idx, row in hottest.iterrows():
        print(f"      {row['date']}: {row['avg_temp']:.1f}°C")
    
    print("\n   5-Самых холодных дней:")
    for idx, row in coldest.iterrows():
        print(f"      {row['date']}: {row['avg_temp']:.1f}°C")
    
    result_path = '/tmp/temperature_transformed.parquet'
    df.to_parquet(result_path, index=False)
    
    print("\n" + "="*60)
    print("="*60)
    
    return {
        'processed_records': len(df),
        'hottest_days': hottest.to_dict('records'),
        'coldest_days': coldest.to_dict('records'),
        'result_path': result_path
    }

def load_results(**context):
    result = context['ti'].xcom_pull(task_ids='transform_temperature')
    
    print(f"Обработано записей: {result['processed_records']}")
    
with DAG(
    'simple_temperature_task',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@once',
    catchup=False,
    default_args={
        'owner': 'student',
        'retries': 0
    }
) as dag:
    
    task1 = PythonOperator(
        task_id='extract_temperature',
        python_callable=extract_temperature
    )
    
    task2 = PythonOperator(
        task_id='transform_temperature',
        python_callable=transform_temperature
    )
    
    task3 = PythonOperator(
        task_id='load_results',
        python_callable=load_results
    ) 

    task1 >> task2 >> task3