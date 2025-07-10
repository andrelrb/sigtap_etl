# Nome do arquivo: step_04_executor_scripts_sql.py
# Versao do Arquivo: 5.1.1 (Ignora 'log_erro' na limpeza)
# Descrição: Executa os scripts da pasta mais recente e limpa as pastas antigas,
#            preservando a pasta 'log_erro'.

import os
import datetime
import psycopg2
import psycopg2.errors
from psycopg2 import sql
import re
import sys
import configparser
import traceback
import shutil

# --- CONFIGURAÇÕES GERAIS E ESTÁTICAS ---
CODIFICACAO_ARQUIVOS_SQL = 'utf-8'
COLUNA_VERSION_FK_EM_TABELAS = 'version_id'
TABELA_VERSION_NOME = 'tb_version'
ORDEM_PROCESSAMENTO_TABELAS_BASE = [
    'tb_detalhe', 'tb_ocupacao', 'tb_financiamento', 'tb_habilitacao', 'tb_tipo_leito', 'tb_grupo',
    'tb_registro', 'tb_rede_atencao', 'tb_renases', 'tb_regra_condicionada', 'tb_rubrica', 'tb_cid',
    'tb_modalidade', 'tb_grupo_habilitacao', 'tb_servico', 'tb_tuss', 'tb_sub_grupo', 'tb_componente_rede',
    'tb_servico_classificacao', 'tb_descricao_detalhe', 'tb_forma_organizacao', 'tb_procedimento',
    'tb_descricao', 'rl_procedimento_sia_sih', 'tb_sia_sih', 'rl_procedimento_habilitacao',
    'rl_procedimento_renases', 'rl_procedimento_tuss', 'rl_procedimento_comp_rede', 'rl_procedimento_cid',
    'rl_procedimento_modalidade', 'rl_procedimento_ocupacao', 'rl_procedimento_servico',
    'rl_procedimento_leito', 'rl_procedimento_detalhe', 'rl_excecao_compatibilidade',
    'rl_procedimento_registro', 'rl_procedimento_compativel', 'rl_procedimento_regra_cond',
    'rl_procedimento_origem', 'rl_procedimento_incremento'
]
ORDEM_PROCESSAMENTO_ARQUIVOS_SQL = [f"{tabela_base}_insert.sql" for tabela_base in ORDEM_PROCESSAMENTO_TABELAS_BASE]

# --- FUNÇÕES DE AUTOMAÇÃO E AUXILIARES ---

def obter_raiz_projeto():
    """Encontra e retorna o caminho absoluto da pasta raiz do projeto."""
    project_root = os.path.dirname(os.path.abspath(__file__))
    while not any(os.path.exists(os.path.join(project_root, marker)) for marker in ['README.md', '.git', 'config.ini']):
        parent = os.path.dirname(project_root)
        if parent == project_root: raise FileNotFoundError("Não foi possível localizar a pasta raiz do projeto.")
        project_root = parent
    return project_root

def carregar_db_config_do_ini(project_root):
    """Lê as configurações do banco de dados do arquivo config.ini."""
    config_path = os.path.join(project_root, 'config.ini')
    if not os.path.exists(config_path): raise FileNotFoundError(f"Erro Crítico: Arquivo 'config.ini' não encontrado em: {project_root}")
    parser = configparser.ConfigParser()
    parser.read(config_path)
    if 'DATABASE' not in parser: raise ValueError("Erro Crítico: Seção [DATABASE] não encontrada no arquivo config.ini")
    db_config = dict(parser['DATABASE'])
    print("Configurações do banco lidas com sucesso do config.ini.")
    return db_config

def obter_pasta_mais_recente(caminho_base):
    """Encontra e retorna o caminho da subpasta mais recente em um diretório base."""
    if not os.path.isdir(caminho_base): raise FileNotFoundError(f"Erro Crítico: A pasta base '{os.path.basename(caminho_base)}' não foi encontrada em '{caminho_base}'.")
    subpastas = [d for d in os.listdir(caminho_base) if os.path.isdir(os.path.join(caminho_base, d))]
    if not subpastas: raise FileNotFoundError(f"Nenhuma subpasta encontrada em '{caminho_base}'.")
    nome_pasta_recente = max(subpastas, key=lambda d: os.path.getmtime(os.path.join(caminho_base, d)))
    return os.path.join(caminho_base, nome_pasta_recente), nome_pasta_recente

def conectar_bd(db_config):
    """Estabelece conexão com o banco de dados."""
    try:
        conn = psycopg2.connect(**db_config, client_encoding='utf8')
        print(f"Conectado ao banco de dados '{db_config['dbname']}' em '{db_config['host']}'.")
        return conn
    except psycopg2.Error as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}")
        return None

def verificar_dados_existentes(conn, tabela, version_id):
    """Verifica se já existem dados para uma tabela com um version_id específico."""
    try:
        with conn.cursor() as cur:
            query = sql.SQL("SELECT 1 FROM public.{table} WHERE {version_col} = %s LIMIT 1").format(
                table=sql.Identifier(tabela),
                version_col=sql.Identifier(COLUNA_VERSION_FK_EM_TABELAS)
            )
            cur.execute(query, (version_id,));
            return cur.fetchone() is not None
    except psycopg2.errors.UndefinedTable:
        return False
    except psycopg2.Error as e:
        print(f"  AVISO: Erro ao verificar dados existentes para {tabela}: {e.pgerror}")
        return True

def executar_scripts_sql_da_pasta(conn, caminho_pasta_scripts, version_id_da_pasta):
    """Executa todos os scripts .sql de uma pasta, em ordem definida."""
    print(f"\n--- FASE 2: Executando scripts da pasta ---")
    print(f"Caminho: {caminho_pasta_scripts}")
    print(f"Associado ao Version ID: {version_id_da_pasta}")

    arquivos_na_pasta = set(os.listdir(caminho_pasta_scripts))
    scripts_a_executar = [f for f in ORDEM_PROCESSAMENTO_ARQUIVOS_SQL if f in arquivos_na_pasta]

    sucesso_lista, falha_lista, pulados_lista = [], [], []
    for nome_arquivo_sql in scripts_a_executar:
        tabela_alvo_base = nome_arquivo_sql.replace("_insert.sql", "")
        print(f"\nProcessando script: {nome_arquivo_sql} (Tabela Alvo: {tabela_alvo_base})")

        if verificar_dados_existentes(conn, tabela_alvo_base, version_id_da_pasta):
            print(f"  AVISO: Dados já existem para '{tabela_alvo_base}' com version_id {version_id_da_pasta}. Script pulado.")
            pulados_lista.append(nome_arquivo_sql)
            conn.rollback()
            continue

        caminho_script_sql = os.path.join(caminho_pasta_scripts, nome_arquivo_sql)
        try:
            with open(caminho_script_sql, 'r', encoding=CODIFICACAO_ARQUIVOS_SQL) as f_script:
                sql_content = f_script.read().strip()

            if not sql_content or not any(line.strip().upper().startswith('INSERT') for line in sql_content.split(';')):
                print(f"  AVISO: Arquivo SQL '{nome_arquivo_sql}' está vazio ou não contém INSERTs. Pulando.")
                pulados_lista.append(f"{nome_arquivo_sql} (vazio)")
                continue

            with conn.cursor() as cur:
                print(f"  Executando {nome_arquivo_sql}...")
                cur.execute(sql_content)

            conn.commit()
            print(f"  Script {nome_arquivo_sql} executado com sucesso.")
            sucesso_lista.append(nome_arquivo_sql)

        except (psycopg2.Error, Exception) as e:
            conn.rollback()
            falha_lista.append(nome_arquivo_sql)
            error_message = getattr(e, 'pgerror', str(e))
            print(f"  ERRO ao executar {nome_arquivo_sql}: {error_message}")
            print(f"  Transação para {nome_arquivo_sql} revertida.")

    return sucesso_lista, falha_lista, pulados_lista

# --- ALTERAÇÃO PRINCIPAL APLICADA AQUI ---
def limpar_pastas_antigas(caminho_base, pasta_a_manter):
    """
    Apaga todas as subpastas em um diretório, exceto a pasta especificada e a pasta 'log_erro'.
    """
    print(f"\n--- FASE 3: Limpeza da pasta '{os.path.basename(caminho_base)}' ---")
    if not os.path.isdir(caminho_base):
        print(f"  Aviso: Pasta base para limpeza não encontrada: '{caminho_base}'.")
        return

    pasta_a_manter_abs = os.path.abspath(pasta_a_manter)
    print(f"  Mantendo a pasta mais recente: {os.path.basename(pasta_a_manter_abs)}")

    for nome_item in os.listdir(caminho_base):
        # Nova regra para ignorar a pasta de logs
        if nome_item.lower() == 'log_erro':
            print(f"  Ignorando pasta de logs: {nome_item}")
            continue

        caminho_item = os.path.abspath(os.path.join(caminho_base, nome_item))

        if os.path.isdir(caminho_item) and caminho_item != pasta_a_manter_abs:
            try:
                shutil.rmtree(caminho_item)
                print(f"  Pasta antiga removida: {nome_item}")
            except OSError as e:
                print(f"  ERRO ao remover pasta antiga '{nome_item}': {e}")
# --- FIM DA ALTERAÇÃO PRINCIPAL ---

def main():
    """Função principal que orquestra todo o processo de execução e limpeza."""
    print("--- Executor de Scripts SQL SIGTAP v5.1.1 (Automatizado com Limpeza Segura) ---")
    project_root = None
    try:
        project_root = obter_raiz_projeto()
        print(f"\nRaiz do projeto detectada: {project_root}")

        db_config = carregar_db_config_do_ini(project_root)
        caminho_scripts, nome_pasta_sel = obter_pasta_mais_recente(os.path.join(project_root, 'insert_db'))

        match_info = re.search(r'versaoID_(\d+)', nome_pasta_sel)
        if not match_info: raise ValueError(f"Não foi possível extrair 'versionID' de '{nome_pasta_sel}'.")
        version_id_pasta = match_info.group(1)

    except (FileNotFoundError, ValueError) as e:
        print(f"\nERRO CRÍTICO na configuração: {e}\nProcesso abortado.")
        return

    conn = conectar_bd(db_config)
    if not conn: return

    try:
        sucesso, falha, pulados = executar_scripts_sql_da_pasta(conn, caminho_scripts, version_id_pasta)

        print("\n--- Resumo da Execução ---")
        print(f"Pasta: {nome_pasta_sel}\nVersion ID Carga: {version_id_pasta}")
        if sucesso: print(f"\n{len(sucesso)} Script(s) com SUCESSO:\n  - " + "\n  - ".join(sucesso))
        if pulados: print(f"\n{len(pulados)} Script(s) PULADOS:\n  - " + "\n  - ".join(pulados))
        if falha: print(f"\n{len(falha)} Script(s) com FALHA:\n  - " + "\n  - ".join(falha))

    except Exception as e:
        print(f"Erro inesperado no processo principal: {e}")
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            print("\nConexão com o banco de dados fechada.")

    if project_root:
        try:
            limpar_pastas_antigas(os.path.join(project_root, 'insert_db'), caminho_scripts)

            pasta_create_db = os.path.join(project_root, 'create_db')
            if os.path.isdir(pasta_create_db):
                print(f"\n--- FASE 3: Limpeza da pasta 'create_db' ---")
                itens_create_db = [os.path.join(pasta_create_db, f) for f in os.listdir(pasta_create_db)]
                if len(itens_create_db) > 1:
                    item_mais_recente = max(itens_create_db, key=os.path.getmtime)
                    print(f"  Mantendo o arquivo mais recente: {os.path.basename(item_mais_recente)}")
                    for item in itens_create_db:
                        if item != item_mais_recente:
                            try:
                                if os.path.isdir(item): shutil.rmtree(item)
                                else: os.remove(item)
                                print(f"  Item antigo removido: {os.path.basename(item)}")
                            except OSError as e:
                                print(f"  ERRO ao remover item antigo '{os.path.basename(item)}': {e}")

        except Exception as e_clean:
            print(f"\nERRO durante a fase de limpeza: {e_clean}")
            traceback.print_exc()

    print("\n--- PROCESSO DE EXECUÇÃO E LIMPEZA FINALIZADO ---")

if __name__ == '__main__':
    main()