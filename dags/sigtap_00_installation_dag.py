# dags/sigtap_00_installation_dag.py
# Versão: 1.0
# Descrição: DAG do Airflow para executar o pipeline completo de Instalação,
#            que inclui a extração de dados e a criação do schema do banco.

from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# O caminho para os scripts DENTRO do contêiner do Airflow.
SCRIPTS_PATH = "/opt/airflow/src"

with DAG(
    dag_id="sigtap_00_installation",
    schedule=None,  # Para ser disparado manualmente, não tem agendamento
    start_date=pendulum.datetime(2025, 7, 25, tz="America/Recife"),
    catchup=False,
    tags=["sigtap", "etl", "installation"],
    doc_md="""
    ### Pipeline de Instalação e Schema do SIGTAP

    Este DAG executa as etapas iniciais necessárias para configurar o ambiente:
    1.  **Extração**: Baixa e descompacta os dados mais recentes do FTP.
    2.  **Geração de Schema**: Cria o script SQL (DDL) para a estrutura das tabelas.
    3.  **Execução de Schema**: Aplica o script no banco de dados para criar as tabelas.
    """,
) as dag:
    
    # Tarefa 1: Baixar e Descompactar os Dados
    task_extraction = BashOperator(
        task_id="extracao_e_descompactacao",
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

    # Definindo a ordem de execução das tarefas
    # A tarefa 2 só começa após o sucesso da 1, e a 3 só após o sucesso da 2.
    task_extraction >> task_generate_schema >> task_execute_schema