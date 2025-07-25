# dags/sigtap_etl_dag.py
# Versão: 1.1 (Correção de caminhos para ambiente Docker)
# Descrição: DAG do Airflow que usa os caminhos corretos de dentro do contêiner.

from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# O caminho para os scripts DENTRO do contêiner do Airflow.
# Mapeamos a pasta 'src' do seu projeto para '/opt/airflow/src' no docker-compose.yml.
SCRIPTS_PATH = "/opt/airflow/src"

with DAG(
    dag_id="sigtap_etl_pipeline",
    schedule=None,  # Para ser disparado manualmente
    start_date=pendulum.datetime(2025, 7, 24, tz="America/Recife"),
    catchup=False,
    tags=["sigtap", "etl"],
    doc_md="""
    ### Pipeline ETL SIGTAP

    Este DAG orquestra o processo completo de ETL para os dados do SIGTAP,
    executando os scripts que estão dentro do ambiente Docker.
    """,
) as dag:
    
    # Tarefa 1: Baixar e Descompactar os Dados
    task_extraction = BashOperator(
        task_id="extracao_e_descompactacao",
        # O comando agora usa 'python' (que já está no PATH do contêiner)
        # e o caminho correto para o script.
        bash_command=f"python {SCRIPTS_PATH}/step_00_extraction.py",
    )

    # Tarefa 2: Gerar o Script do Schema do Banco
    task_generate_schema = BashOperator(
        task_id="gerar_schema_sql",
        bash_command=f"python {SCRIPTS_PATH}/Step_01_generate_schema_db_sigtap_sql.py",
    )

    # Tarefa 3: Executar o Schema no Banco
    task_execute_schema = BashOperator(
        task_id="executar_schema_no_banco",
        bash_command=f"python {SCRIPTS_PATH}/step_02_create_table_sigtap.py",
    )

    # Tarefa 4: Gerar os Scripts de INSERT
    task_generate_inserts = BashOperator(
        task_id="gerar_scripts_de_insert",
        bash_command=f"python {SCRIPTS_PATH}/step_03_unified_generate_inserts.py",
    )

    # Tarefa 5: Executar os Scripts de INSERT no Banco
    task_execute_inserts = BashOperator(
        task_id="executar_scripts_de_insert",
        bash_command=f"python {SCRIPTS_PATH}/step_04_executor_scripts_sql.py",
    )

    # Definindo a ordem de execução das tarefas
    task_extraction >> task_generate_schema >> task_execute_schema >> task_generate_inserts >> task_execute_inserts