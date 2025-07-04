import os
import configparser
import psycopg2
import sys
from pathlib import Path

# --- CONFIGURAÇÕES GLOBAIS ---
# Define o caminho base como o diretório pai da pasta 'src'
# Isso garante que o script encontre os arquivos na raiz do projeto (GerarSQL_V2).
BASE_DIR = Path(__file__).resolve().parent.parent

CONFIG_FILE_PATH = BASE_DIR / 'config.ini'
SQL_FILES_DIR = BASE_DIR / 'create_db'

def ler_configuracao_banco(config_path):
    """
    Lê o arquivo de configuração .ini e retorna um dicionário com as credenciais do banco.
    """
    if not config_path.exists():
        print(f"ERRO: Arquivo de configuração não encontrado em: {config_path}")
        sys.exit(1)

    config = configparser.ConfigParser()
    config.read(config_path)

    try:
        # Tenta ler a seção [DATABASE] do arquivo .ini
        db_params = dict(config['DATABASE']) # <-- ALTERAÇÃO AQUI
        print("Arquivo de configuração lido com sucesso.")
        return db_params
    except KeyError:
        # Atualiza a mensagem de erro para refletir a nova seção
        print("ERRO: A seção [DATABASE] não foi encontrada no arquivo config.ini.") # <-- ALTERAÇÃO AQUI
        print("Verifique se o arquivo está formatado corretamente.")
        sys.exit(1)

def encontrar_arquivo_sql_mais_recente(diretorio_sql):
    """
    Encontra o arquivo .sql modificado mais recentemente no diretório especificado.
    """
    if not diretorio_sql.exists() or not diretorio_sql.is_dir():
        print(f"ERRO: O diretório de scripts SQL não foi encontrado em: {diretorio_sql}")
        sys.exit(1)

    try:
        arquivos_sql = [f for f in diretorio_sql.iterdir() if f.is_file() and f.suffix == '.sql']

        if not arquivos_sql:
            print(f"Nenhum arquivo .sql encontrado no diretório: {diretorio_sql}")
            return None

        arquivo_mais_recente = max(arquivos_sql, key=lambda f: f.stat().st_mtime)
        print(f"Arquivo SQL mais recente encontrado: {arquivo_mais_recente.name}")
        return arquivo_mais_recente
    except Exception as e:
        print(f"Ocorreu um erro ao procurar o arquivo SQL: {e}")
        sys.exit(1)


def executar_script_sql(db_params, sql_file_path):
    """
    Conecta ao banco de dados PostgreSQL e executa o conteúdo de um arquivo SQL.
    """
    conn = None
    cursor = None
    try:
        print("Tentando conectar ao banco de dados PostgreSQL...")
        conn = psycopg2.connect(**db_params)
        conn.autocommit = True
        cursor = conn.cursor()
        print("Conexão estabelecida com sucesso.")

        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
            print(f"Lendo e executando o script: {sql_file_path.name}...")
            cursor.execute(sql_script)

        print("Script SQL executado com sucesso! As tabelas foram criadas.")

    except psycopg2.Error as e:
        print(f"ERRO de banco de dados: {e}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            print("Conexão com o banco de dados fechada.")

def main():
    """
    Função principal que orquestra a execução do script.
    """
    print("--- INICIANDO ETAPA 02: CRIAÇÃO DAS TABELAS SIGTAP ---")
    
    params_banco = ler_configuracao_banco(CONFIG_FILE_PATH)
    arquivo_sql_para_rodar = encontrar_arquivo_sql_mais_recente(SQL_FILES_DIR)

    if arquivo_sql_para_rodar:
        executar_script_sql(params_banco, arquivo_sql_para_rodar)
    else:
        print("Nenhum script SQL para executar. Encerrando o processo.")

    print("--- ETAPA 02 FINALIZADA ---")

if __name__ == "__main__":
    main()