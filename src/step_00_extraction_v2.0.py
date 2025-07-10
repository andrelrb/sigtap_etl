# Módulo para baixar e descompactar dados do FTP
# Versão: 2.0
# Descrição:
# Baixa o arquivo .zip mais recente do FTP e o descompacta em uma
# subpasta com o mesmo nome do arquivo .zip, para melhor organização.

import logging
from ftplib import FTP
import os
import zipfile
from datetime import datetime
from dateutil.relativedelta import relativedelta

def find_latest_file_and_competencia(ftp):
    """
    Procura no FTP o arquivo da Tabela Unificada mais recente.
    Tenta o mês atual e, se não achar, tenta o mês anterior.
    """
    logging.info("Buscando o arquivo da Tabela Unificada mais recente no FTP...")
    remote_files = ftp.nlst()

    def find_file(target_date):
        competencia_yyyymm = target_date.strftime('%Y%m')
        prefix_to_find = f"TabelaUnificada_{competencia_yyyymm}"
        logging.info(f"Procurando por arquivos que comecem com: '{prefix_to_find}'")
        
        for file_name in remote_files:
            if file_name.startswith(prefix_to_find):
                logging.info(f"SUCESSO! Arquivo encontrado: {file_name}")
                return file_name, competencia_yyyymm
        return None, None

    current_date = datetime.now()
    file_name, competencia = find_file(current_date)
    if file_name:
        return file_name, competencia

    logging.warning(f"Nenhum arquivo encontrado para a competência {current_date.strftime('%Y%m')}. Tentando mês anterior...")
    previous_month_date = current_date - relativedelta(months=1)
    file_name, competencia = find_file(previous_month_date)
    if file_name:
        return file_name, competencia
            
    return None, None

def download_and_unzip(config):
    """
    Etapa 0: Conecta ao FTP, encontra o arquivo, baixa e descompacta para uma pasta específica.
    """
    logging.info("--- INICIANDO ETAPA 0: EXTRAÇÃO DE DADOS DO FTP ---")
    ftp_config = config['FTP']
    path_config = config['PATHS']

    os.makedirs(path_config['download_dir'], exist_ok=True)
    
    conn = None
    try:
        logging.info(f"Conectando ao servidor FTP: {ftp_config['server']}...")
        conn = FTP(ftp_config['server'], timeout=30)
        conn.login(user=ftp_config['user'], passwd=ftp_config['password'])
        conn.cwd(ftp_config['path'])

        zip_file_name, competencia = find_latest_file_and_competencia(conn)

        if not zip_file_name:
            logging.error("Não foi possível encontrar um arquivo da Tabela Unificada para o mês atual ou anterior. Abortando.")
            conn.quit()
            return None, None, None

        zip_file_path = os.path.join(path_config['download_dir'], zip_file_name)

        logging.info(f"Baixando arquivo '{zip_file_name}'...")
        with open(zip_file_path, 'wb') as local_file:
            conn.retrbinary(f"RETR {zip_file_name}", local_file.write)
        conn.quit()
        logging.info(f"Download concluído. Arquivo salvo em: {zip_file_path}")

        # --- NOVA REGRA AQUI ---
        # Cria um nome para a pasta de destino baseado no nome do arquivo .zip
        unzip_target_folder_name = os.path.splitext(zip_file_name)[0]
        unzip_full_path = os.path.join(path_config['unzip_dir'], unzip_target_folder_name)
        
        # Cria a pasta de destino específica
        os.makedirs(unzip_full_path, exist_ok=True)
        
        logging.info(f"Descompactando arquivo para a pasta específica: {unzip_full_path}")
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(unzip_full_path)
        logging.info("Descompactação concluída com sucesso.")
        
        # Retorna o caminho completo da pasta descompactada para as próximas etapas
        return competencia, unzip_full_path, path_config['sql_inserts_dir']

    except Exception as e:
        logging.error(f"Ocorreu um erro durante a extração: {e}", exc_info=True)
        if conn:
            conn.quit()
        return None, None, None