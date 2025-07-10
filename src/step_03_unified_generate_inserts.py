# Nome do arquivo: step_03_unified_generate_inserts.py
# Versão: 2.3.0 (Permite recarregar competências existentes)
# Descrição: Script que cria um novo registro de versão (mesmo para competências existentes)
#            e gera os arquivos SQL de INSERT. Lê a configuração do banco do config.ini.

import os
import csv
import datetime
import re
import sys
import traceback
import configparser

try:
    import psycopg2
    from psycopg2 import sql
except ImportError:
    print("ERRO CRÍTICO: A biblioteca 'psycopg2' não está instalada. Execute: pip install psycopg2-binary")
    sys.exit(1)

# --- CONFIGURAÇÕES GERAIS E ESTÁTICAS ---
SUFIXO_LAYOUT = '_layout.txt'
COLUNA_VERSION_FK = 'version_id'
CODIFICACAO_ARQUIVOS_DADOS = 'latin-1'
ARQUIVOS_RECONSTRUIR_LINHA = ['tb_componente_rede.txt']
ORDEM_PROCESSAMENTO_TABELAS = [
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

# --- FUNÇÕES DE AUTOMAÇÃO E AUXILIARES ---

def obter_raiz_projeto():
    """Encontra e retorna o caminho absoluto da pasta raiz do projeto."""
    project_root = os.path.dirname(os.path.abspath(__file__))
    while not any(os.path.exists(os.path.join(project_root, marker)) for marker in ['README.md', '.git', 'config.ini']):
        parent = os.path.dirname(project_root)
        if parent == project_root:
            raise FileNotFoundError("Não foi possível localizar a pasta raiz do projeto.")
        project_root = parent
    return project_root

def carregar_db_config_do_ini(project_root):
    """Lê as configurações do banco de dados do arquivo config.ini."""
    config_path = os.path.join(project_root, 'config.ini')
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Erro Crítico: Arquivo 'config.ini' não encontrado em: {project_root}")
    parser = configparser.ConfigParser()
    parser.read(config_path)
    if 'DATABASE' not in parser:
        raise ValueError("Erro Crítico: Seção [DATABASE] não encontrada no arquivo config.ini")
    db_config = dict(parser['DATABASE'])
    print("Configurações do banco lidas com sucesso do config.ini.")
    return db_config

def setup_automatico_paths(project_root):
    """Detecta e configura dinamicamente os caminhos de entrada (dados) e saída (SQL)."""
    configs = {}
    unzipped_base_dir = os.path.join(project_root, 'downloads', 'unzipped')
    if not os.path.isdir(unzipped_base_dir): raise FileNotFoundError(f"Pasta 'downloads/unzipped' não encontrada.")
    all_data_dirs = [d for d in os.listdir(unzipped_base_dir) if os.path.isdir(os.path.join(unzipped_base_dir, d))]
    if not all_data_dirs: raise FileNotFoundError(f"Nenhuma pasta de dados encontrada em '{unzipped_base_dir}'.")
    latest_data_dir_name = max(all_data_dirs, key=lambda d: os.path.getmtime(os.path.join(unzipped_base_dir, d)))
    configs['CAMINHO_PASTA_DADOS'] = os.path.join(unzipped_base_dir, latest_data_dir_name)
    configs['CAMINHO_BASE_SAIDA_SQL'] = os.path.join(project_root, 'insert_db')
    os.makedirs(configs['CAMINHO_BASE_SAIDA_SQL'], exist_ok=True)
    print(f"Pasta de dados detectada: {configs['CAMINHO_PASTA_DADOS']}")
    print(f"Pasta de saída SQL: {configs['CAMINHO_BASE_SAIDA_SQL']}")
    return configs

def conectar_bd(db_params):
    """Conecta ao banco de dados PostgreSQL."""
    try:
        conn = psycopg2.connect(**db_params, client_encoding='utf8')
        return conn
    except psycopg2.Error as e:
        print(f"ERRO CRÍTICO ao conectar ao PostgreSQL: {e}")
        return None

def extrair_competencia(caminho_pasta_dados):
    """Extrai a competência (AAAAMM) do nome da pasta."""
    match = re.search(r'_(\d{6})', os.path.basename(caminho_pasta_dados))
    return match.group(1) if match else None

# --- ALTERAÇÃO PRINCIPAL APLICADA AQUI ---
def run_version_creation_step(conn, competencia):
    """
    Cria um novo registro de versão. 
    AGORA, esta função permite a criação de um novo version_id
    mesmo que a competência já exista.
    """
    print("\n--- FASE 1: CRIAÇÃO DO REGISTRO DE VERSÃO ---")
    if not competencia:
        print("ERRO: Competência não pôde ser extraída. Impossível criar registro de versão.")
        return None, None
    try:
        with conn.cursor() as cur:
            # A VERIFICAÇÃO DE EXISTÊNCIA FOI REMOVIDA PARA ATENDER À NOVA REGRA.
            # O script agora sempre criará uma nova entrada de versão.
            
            print(f"Criando novo registro de versão para a competência '{competencia}'...")
            
            # O comando de INSERT permanece o mesmo, ele sempre criará uma nova linha
            # com um novo 'id' graças ao BIGSERIAL.
            insert_query = sql.SQL("""
                INSERT INTO public.tb_version (competencia, created_at) 
                VALUES (%s, %s) 
                RETURNING id;
            """)
            
            cur.execute(insert_query, (competencia, datetime.datetime.now()))
            
            # Pega o novo ID que o banco acabou de criar
            new_id = cur.fetchone()[0]
            
            conn.commit()
            print(f"SUCESSO! Novo registro de versão criado. Version ID: {new_id}, Competência: {competencia}")
            return new_id, competencia
            
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERRO de banco de dados na FASE 1: {e}")
        traceback.print_exc()
        return None, None
# --- FIM DA ALTERAÇÃO PRINCIPAL ---

def parse_layout(layout_filepath):
    """Lê um arquivo de layout e retorna a estrutura das colunas."""
    colunas_layout = []
    try:
        with open(layout_filepath, 'r', encoding=CODIFICACAO_ARQUIVOS_DADOS) as f:
            reader = csv.reader(f); next(reader)
            for row in reader:
                if len(row) >= 5:
                    nome_coluna = row[0].strip().lower().replace(' ', '_').replace('-', '_')
                    inicio, fim = int(row[2]) - 1, int(row[3])
                    colunas_layout.append({'nome': nome_coluna, 'inicio': inicio, 'fim': fim})
    except (FileNotFoundError, ValueError, IndexError):
        return None
    return colunas_layout

def gerar_sql_para_arquivo_dados(conn, data_filepath, layout_filepath, caminho_saida_sql, tabela_nome_base, version_id):
    """Gera um arquivo .sql com os comandos INSERT para um arquivo de dados."""
    colunas_layout = parse_layout(layout_filepath)
    if not colunas_layout: return False

    nome_arquivo_sql = f"{tabela_nome_base}_insert.sql"
    caminho_completo_sql = os.path.join(caminho_saida_sql, nome_arquivo_sql)
    colunas_para_insert = [col['nome'] for col in colunas_layout] + [COLUNA_VERSION_FK]
    colunas_sql_str = ", ".join([f'"{c}"' for c in colunas_para_insert])
    table_name_sql = sql.SQL("public.{}").format(sql.Identifier(tabela_nome_base))
    
    try:
        with open(data_filepath, 'r', encoding=CODIFICACAO_ARQUIVOS_DADOS) as f_dados, \
             open(caminho_completo_sql, 'w', encoding='utf-8') as f_sql:
            
            print(f"  -> Gerando: {nome_arquivo_sql}")
            f_sql.write(f"-- INSERTS para a tabela {table_name_sql.as_string(conn)} com version_id = {version_id}\n\n")
            
            linhas_geradas = 0
            for linha_raw in f_dados:
                linha = linha_raw.rstrip('\n')
                if os.path.basename(data_filepath) in ARQUIVOS_RECONSTRUIR_LINHA and len(linha) < 206:
                    try: linha += next(f_dados).rstrip('\n')
                    except StopIteration: pass
                
                valores_formatados = []
                for col in colunas_layout:
                    valor_raw = linha[col['inicio']:col['fim']].strip()
                    valores_formatados.append("NULL" if valor_raw == '' else f"'{valor_raw.replace("'", "''")}'")
                
                valores_formatados.append(str(version_id))
                
                f_sql.write(f"INSERT INTO {table_name_sql.as_string(conn)} ({colunas_sql_str}) VALUES ({', '.join(valores_formatados)});\n")
                linhas_geradas += 1
            
            if linhas_geradas > 0:
                 print(f"    {linhas_geradas} linhas de INSERT geradas.")
        return True
    except Exception as e:
        print(f"ERRO ao gerar SQL para {os.path.basename(data_filepath)}: {e}")
        return False

def run_sql_generation_step(conn, configs, version_id, competencia):
    """Orquestra a geração de todos os arquivos SQL de INSERT."""
    print("\n--- FASE 2: GERAÇÃO DOS SCRIPTS SQL DE INSERT ---")
    data_hoje_str = datetime.date.today().strftime('%Y%m%d')
    nome_pasta_saida = f"insert_{data_hoje_str}_versaoID_{version_id}_competencia_{competencia}"
    caminho_pasta_saida_final = os.path.join(configs['CAMINHO_BASE_SAIDA_SQL'], nome_pasta_saida)
    os.makedirs(caminho_pasta_saida_final, exist_ok=True)
    print(f"Pasta de saída: {caminho_pasta_saida_final}")
    
    sucessos, erros = 0, []
    for nome_tabela in ORDEM_PROCESSAMENTO_TABELAS:
        data_file = os.path.join(configs['CAMINHO_PASTA_DADOS'], f"{nome_tabela}.txt")
        layout_file = os.path.join(configs['CAMINHO_PASTA_DADOS'], f"{nome_tabela}{SUFIXO_LAYOUT}")
        
        if os.path.exists(data_file) and os.path.exists(layout_file):
            print(f"Processando tabela: {nome_tabela}...")
            if gerar_sql_para_arquivo_dados(conn, data_file, layout_file, caminho_pasta_saida_final, nome_tabela, version_id):
                sucessos += 1
            else:
                erros.append(nome_tabela)
                
    print("\n--- Resumo da Geração de Scripts ---")
    print(f"Arquivos processados com sucesso: {sucessos}")
    if erros: print(f"Tabelas com erro no processamento: {', '.join(erros)}")
    print(f"Verifique os arquivos SQL em: {caminho_pasta_saida_final}")

def main():
    """Função principal que orquestra todo o processo."""
    print("--- INICIANDO SCRIPT UNIFICADO DE GERAÇÃO DE INSERTS (v2.3.0) ---")
    try:
        project_root = obter_raiz_projeto()
        print(f"Raiz do projeto detectada: {project_root}")
        
        db_config = carregar_db_config_do_ini(project_root)
        path_configs = setup_automatico_paths(project_root)

        competencia_atual = extrair_competencia(path_configs['CAMINHO_PASTA_DADOS'])
        if not competencia_atual: raise ValueError("Não foi possível determinar a competência.")

    except (FileNotFoundError, ValueError) as e:
        print(f"\nERRO CRÍTICO na configuração: {e}\nProcesso abortado.")
        return

    conn = conectar_bd(db_config)
    if not conn: return

    try:
        version_id, competencia = run_version_creation_step(conn, competencia_atual)
        
        if version_id:
            run_sql_generation_step(conn, path_configs, version_id, competencia)

    except Exception as e:
        print(f"Erro inesperado no processo principal: {e}")
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            print("\nConexão com o banco de dados fechada.")
            
    print("\n--- PROCESSO UNIFICADO FINALIZADO ---")

if __name__ == '__main__':
    main()