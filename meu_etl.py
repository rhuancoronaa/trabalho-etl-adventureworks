from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os

os.environ["AIRFLOW_CONN_POSTGRES_MASTER"] = "postgresql+psycopg2://airflow:airflow@postgres/airflow"

DATA_PATH = '/opt/airflow/data'

def processar_adventure_works():
    hook = PostgresHook(postgres_conn_id='postgres_master')
    engine = hook.get_sqlalchemy_engine()

    try:
        df_sales = pd.read_csv(f'{DATA_PATH}/SalesOrder.csv')
        df_customer = pd.read_csv(f'{DATA_PATH}/Customer.csv')
        df_product = pd.read_csv(f'{DATA_PATH}/Product.csv')
    except Exception as e:
        print(f"Erro: {e}")
        raise

    dim_cliente = df_customer[['CustomerID', 'FirstName', 'LastName', 'City', 'CountryRegionName']].copy()
    dim_cliente.columns = ['id_cliente', 'nome', 'sobrenome', 'cidade', 'pais']
    
    dim_produto = df_product[['ProductID', 'Name', 'ProductNumber', 'Color', 'ListPrice']].copy()
    dim_produto.columns = ['id_produto', 'nome_produto', 'codigo', 'cor', 'preco_lista']

    fato_vendas = df_sales[['SalesOrderID', 'OrderDate', 'CustomerID', 'ProductID', 'OrderQty', 'UnitPrice', 'TotalDue']].copy()
    fato_vendas.columns = ['id_venda', 'data_venda', 'id_cliente', 'id_produto', 'quantidade', 'valor_unitario', 'valor_total']
    
    fato_vendas['valor_liquido'] = fato_vendas['valor_total'] * 0.9 

    dim_cliente.to_sql('dim_cliente', engine, if_exists='replace', index=False)
    dim_produto.to_sql('dim_produto', engine, if_exists='replace', index=False)
    fato_vendas.to_sql('fato_vendas', engine, if_exists='replace', index=False)

with DAG(
    'etl_adventureworks_postgres',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['adventureworks', 'postgres']
) as dag:

    tarefa_etl = PythonOperator(
        task_id='etl_completo_db',
        python_callable=processar_adventure_works
    )