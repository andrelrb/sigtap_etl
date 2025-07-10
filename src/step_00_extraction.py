# Nome do arquivo: step_00_extraction.py
# Versão: 1.0
# Descrição: Baixa e descompacta os dados mais recentes do FTP do DATASUS.
#            Lê as configurações do arquivo config.ini.

import os
import ftplib
import zipfile
import datetime
import configparser
import sys

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
    Função principal que executa o download e a descompactação.
    """
    try:
        ftp_config = config['FTP']
        path_config = config['PATHS']
        
        # Define os caminhos a partir da raiz do projeto
        project_root = obter_raiz_projeto()
        download_dir = os.path.join(project_root, path_config['download_dir'])
        unzip_dir = os.path.join(project_root, path_config['unzip_dir'])

        # Garante que as pastas de destino existam
        os.makedirs(download_dir, exist_ok=True)
        os.makedirs(unzip_dir, exist_ok=True)

        # Determina o nome do arquivo a ser baixado (competência atual)
        competencia = datetime.datetime.now().strftime('%Y%m')
        # O nome do arquivo pode variar, ajuste se necessário
        nome_arquivo_zip = f"TabelaUnificada_{competencia}.zip"
        caminho_local_zip = os.path.join(download_dir, nome_arquivo_zip)

        print(f"--- Iniciando Etapa 0: Extração ---")
        print(f"Conectando ao servidor FTP: {ftp_config['server']}...")

        # Conexão e download do FTP
        with ftplib.FTP(ftp_config['server']) as ftp:
            ftp.login(user=ftp_config['user'], passwd=ftp_config.get('password', ''))
            ftp.cwd(ftp_config['path'])
            
            print(f"Procurando pelo arquivo: {nome_arquivo_zip}")
            with open(caminho_local_zip, 'wb') as f:
                ftp.retrbinary(f"RETR {nome_arquivo_zip}", f.write)
            
            print(f"Download concluído: {caminho_local_zip}")

        # Descompactação
        print(f"Descompactando arquivos para: {unzip_dir}")
        with zipfile.ZipFile(caminho_local_zip, 'r') as zip_ref:
            # Para evitar que os arquivos fiquem dentro de uma subpasta com o mesmo nome
            # vamos extrair e, se necessário, mover
            nome_da_pasta_no_zip = zip_ref.namelist()[0].split('/')[0]
            caminho_extracao_temporario = os.path.join(unzip_dir, "temp_extracao")
            zip_ref.extractall(caminho_extracao_temporario)
            
            # Move os arquivos da subpasta para a pasta de destino final
            pasta_real_dos_arquivos = os.path.join(caminho_extracao_temporario, nome_da_pasta_no_zip)
            pasta_destino_final = os.path.join(unzip_dir, nome_da_pasta_no_zip)

            # Se a pasta de destino já existir, apaga para garantir uma extração limpa
            if os.path.exists(pasta_destino_final):
                import shutil
                shutil.rmtree(pasta_destino_final)
            
            os.rename(pasta_real_dos_arquivos, pasta_destino_final)
            # Limpa a pasta temporária
            import shutil
            shutil.rmtree(caminho_extracao_temporario)

            print("Arquivos descompactados com sucesso.")

        # Limpeza do arquivo .zip
        os.remove(caminho_local_zip)
        print("Arquivo .zip removido.")
        
        return True

    except ftplib.error_perm as e:
        print(f"ERRO DE FTP: {e}")
        print("Verifique as configurações no 'config.ini' ou se o arquivo da competência atual já está disponível no servidor.")
        return False
    except Exception as e:
        print(f"ERRO inesperado na extração: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    try:
        project_root = obter_raiz_projeto()
        config = carregar_config(project_root)
        
        if not download_and_unzip(config):
            # Sai com um código de erro para o orquestrador saber que falhou
            sys.exit(1)
            
    except (FileNotFoundError, ValueError) as e:
        print(f"ERRO DE CONFIGURAÇÃO: {e}")
        sys.exit(1)