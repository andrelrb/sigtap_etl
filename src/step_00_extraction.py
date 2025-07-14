# Nome do arquivo: step_00_extraction.py
# Versão: 1.2 (Adiciona limpeza automática de arquivos .zip antigos)
# Descrição: Baixa o arquivo mais recente do FTP e, ao final, limpa a pasta de
#            downloads, mantendo apenas o arquivo que acabou de ser baixado.

import os
import ftplib
import zipfile
import datetime
import configparser
import sys
import shutil

def obter_raiz_projeto():
    """Encontra e retorna o caminho absoluto da pasta raiz do projeto."""
    project_root = os.path.dirname(os.path.abspath(__file__))
    while not any(os.path.exists(os.path.join(project_root, marker)) for marker in ['README.md', '.git', 'config.ini']):
        parent = os.path.dirname(project_root)
        if parent == project_root:
            raise FileNotFoundError("Não foi possível localizar a pasta raiz do projeto.")
        project_root = parent
    return project_root

def carregar_config(project_root):
    """Carrega as configurações do arquivo config.ini."""
    config_path = os.path.join(project_root, 'config.ini')
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Arquivo 'config.ini' não encontrado em: {project_root}")
    
    config = configparser.ConfigParser()
    config.read(config_path)
    return config

def download_and_unzip(config):
    """
    Função principal que executa o download, descompactação e limpeza.
    """
    caminho_local_zip = None # Inicializa a variável
    try:
        ftp_config = config['FTP']
        path_config = config['PATHS']
        
        project_root = obter_raiz_projeto()
        download_dir = os.path.join(project_root, path_config.get('download_dir', 'downloads'))
        unzip_base_dir = os.path.join(project_root, path_config.get('unzip_dir', 'downloads/unzipped'))
        os.makedirs(download_dir, exist_ok=True)
        os.makedirs(unzip_base_dir, exist_ok=True)

        competencia_atual = datetime.datetime.now().strftime('%Y%m')
        prefixo_arquivo = f"TabelaUnificada_{competencia_atual}"

        print(f"--- Iniciando Etapa 0: Extração ---")
        print(f"Conectando ao servidor FTP: {ftp_config['server']}...")

        nome_arquivo_zip = None
        with ftplib.FTP(ftp_config['server']) as ftp:
            ftp.login(user=ftp_config.get('user', 'anonymous'), passwd=ftp_config.get('password', ''))
            ftp.cwd(ftp_config['path'])
            
            print(f"Listando arquivos no diretório para encontrar o prefixo: {prefixo_arquivo}...")
            lista_arquivos = ftp.nlst()
            
            for nome in lista_arquivos:
                if nome.startswith(prefixo_arquivo) and nome.endswith('.zip'):
                    nome_arquivo_zip = nome
                    break
            
            if not nome_arquivo_zip:
                raise FileNotFoundError(f"Nenhum arquivo encontrado para a competência {competencia_atual} no FTP.")

            print(f"Arquivo encontrado no servidor: {nome_arquivo_zip}")
            caminho_local_zip = os.path.join(download_dir, nome_arquivo_zip)

            print(f"Baixando para: {caminho_local_zip}")
            with open(caminho_local_zip, 'wb') as f:
                ftp.retrbinary(f"RETR {nome_arquivo_zip}", f.write)
            print("Download concluído.")

        pasta_destino_nome = os.path.splitext(nome_arquivo_zip)[0]
        pasta_destino_final = os.path.join(unzip_base_dir, pasta_destino_nome)
        
        print(f"Descompactando para: {pasta_destino_final}")
        if os.path.exists(pasta_destino_final):
            print(f"  Aviso: A pasta de destino '{pasta_destino_nome}' já existe. Removendo para uma extração limpa.")
            shutil.rmtree(pasta_destino_final)
            
        with zipfile.ZipFile(caminho_local_zip, 'r') as zip_ref:
            zip_ref.extractall(pasta_destino_final)
        
        print("Arquivos descompactados com sucesso.")

        # --- NOVA REGRA DE LIMPEZA ---
        print("\n--- Iniciando limpeza de arquivos .zip antigos ---")
        print(f"Diretório de verificação: {download_dir}")
        print(f"Mantendo arquivo mais recente: {nome_arquivo_zip}")
        
        arquivos_removidos = 0
        for item in os.listdir(download_dir):
            if item.endswith('.zip') and item != nome_arquivo_zip:
                try:
                    os.remove(os.path.join(download_dir, item))
                    print(f"  -> Arquivo antigo removido: {item}")
                    arquivos_removidos += 1
                except OSError as e:
                    print(f"  ERRO ao remover o arquivo {item}: {e}")
        
        if arquivos_removidos == 0:
            print("Nenhum arquivo .zip antigo para remover.")
        # --- FIM DA NOVA REGRA ---
        
        return True

    except ftplib.error_perm as e:
        print(f"ERRO DE FTP: {e}")
        return False
    except Exception as e:
        print(f"ERRO inesperado na extração: {e}")
        import traceback
        traceback.print_exc()
        # Se o download falhar, mas um arquivo .zip existir, não tenta apagar
        if caminho_local_zip and os.path.exists(caminho_local_zip):
            print("Mantendo o arquivo baixado devido ao erro na etapa de descompactação/limpeza.")
        return False

if __name__ == '__main__':
    try:
        project_root = obter_raiz_projeto()
        config = carregar_config(project_root)
        
        if not download_and_unzip(config):
            sys.exit(1)
            
    except (FileNotFoundError, ValueError) as e:
        print(f"ERRO DE CONFIGURAÇÃO: {e}")
        sys.exit(1)