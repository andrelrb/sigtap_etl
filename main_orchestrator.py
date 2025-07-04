# Orquestrador principal da automação
# Versão: 3.0
# Descrição: : Orquestrar a execução de todas as etapas em sequência.

# Este script comanda a execução sequencial das etapas do processo de ETL,
#Log: As mensagens de log aparecerão na tela, informando o progresso.

#Etapa 0 (Extração): Irá ao FTP, baixará o arquivo .zip mais recente e o descompactará na pasta downloads/unzipped.

#Etapa 1 (Schema): Irá gerar o arquivo schema_sigtap.sql e executá-lo para criar todas as tabelas no seu banco.

#Etapa 2 (Transformação): Lerá os arquivos .txt e gerará todos os arquivos insert_*.sql na pasta sql_generated.

#Etapa 3 (Carga): Finalmente, executará cada um dos arquivos de INSERT no banco de dados, na ordem correta, para popular as tabelas.

# main_orchestrator.py
import logging
import configparser

def load_config():
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config

def main():
    # Importações feitas dentro da função para evitar erros de import circular
    # e para garantir que o logging seja configurado primeiro.
    from src.utils import setup_logging
    setup_logging()

    from src.step_00_extraction import download_and_unzip
    #from src.step_01_schema import create_and_run_schema
    #from src.step_02_transformation import generate_inserts
    #from src.step_03_load import execute_inserts

    logging.info("====== INICIANDO PROCESSO DE ETL DA TABELA UNIFICADA ======")
    
    try:
        config = load_config()
    except Exception as e:
        logging.critical(f"Erro ao ler o arquivo 'config.ini': {e}")
        return

    # Etapa 0: Extração
    competencia, unzip_dir, sql_inserts_dir = download_and_unzip(config)
    if not all([competencia, unzip_dir, sql_inserts_dir]):
        logging.error("ETAPA 0 (Extração) FALHOU. Processo interrompido.")
        return
    logging.info(f"ETAPA 0 CONCLUÍDA. Competência: {competencia}, Dados em: {unzip_dir}")

    # Etapa 1: Schema
    if not create_and_run_schema(config, unzip_dir):
        logging.error("ETAPA 1 (Schema) FALHOU. Processo interrompido.")
        return
    logging.info("ETAPA 1 CONCLUÍDA. Schema do banco criado/verificado.")

    # Etapa 2: Transformação
    if not generate_inserts(config, unzip_dir, sql_inserts_dir):
        logging.error("ETAPA 2 (Transformação) FALHOU. Processo interrompido.")
        return
    logging.info(f"ETAPA 2 CONCLUÍDA. Arquivos de INSERT gerados em: {sql_inserts_dir}")

    # Etapa 3: Carga
    if not execute_inserts(config, competencia, sql_inserts_dir):
        logging.error("ETAPA 3 (Carga) FALHOU. Processo interrompido.")
        return
    logging.info("ETAPA 3 CONCLUÍDA. Dados carregados no banco.")
    
    logging.info("====== PROCESSO DE ETL FINALIZADO COM SUCESSO ======")

if __name__ == "__main__":
    main()