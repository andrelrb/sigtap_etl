# dags/sigtap_extraction_dag.py
# Versão: 1.0
# Descrição: DAG do Airflow para executar APENAS a tarefa de extração e
#            descompactação dos dados do SIGTAP.

from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# O caminho para os scripts DENTRO do contêiner do Airflow.
SCRIPTS_PATH = "/opt/airflow/src"

with DAG(
    dag_id="sigtap_01_extraction",
    schedule="@monthly",  # Exemplo: agendado para rodar mensalmente
    start_date=pendulum.datetime(2025, 7, 25, tz="America/Recife"),
    catchup=False,
    tags=["sigtap", "etl", "extraction"],
    doc_md="""
    ### Pipeline de Extração SIGTAP

    Este DAG executa a primeira etapa do pipeline:
    - **Extração**: Baixa e descompacta os dados mais recentes do FTP do DATASUS.
    """,
) as dag:
    
    # Tarefa única: Baixar e Descompactar os Dados
    task_extraction = BashOperator(
        task_id="extracao_e_descompactacao",
        bash_command=f"python {SCRIPTS_PATH}/step_00_extraction.py",
    )