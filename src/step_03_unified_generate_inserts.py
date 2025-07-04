# Nome do arquivo: step_03_unified_generate_inserts.py
# Versão: 1.1.0 (Unificado, com correção de NULL para valores vazios)
# Descrição: Script único que cria o registro de versão e gera os arquivos SQL de INSERT.
import os
import csv
import datetime
import re
import sys

try:
    import psycopg2
    from psycopg2 import sql
except ImportError:
    print("--------------------------------------------------------------------")
    print("ERRO CRÍTICO: A biblioteca 'psycopg2' não está instalada.")
    print("Execute o comando: pip install psycopg2-binary")
    print("--------------------------------------------------------------------")
    sys.exit(1)

# --- CONFIGURAÇÃO PRINCIPAL ---
#CAMINHO_PASTA_DADOS = r'G:\Meu Drive\Indra company\SECRETARIA_DE_SAUDE_RECIFE\CAPS\GerarSQL_V2\downloads\unzipped\TabelaUnificada_202505_v2505061938'
CAMINHO_PASTA_DADOS = r'G:\Meu Drive\Indra company\SECRETARIA_DE_SAUDE_RECIFE\CAPS\GerarSQL_V2\downloads\unzipped\TabelaUnificada_202506_v2506061904'

# --- Configurações Gerais ---
CAMINHO_BASE_SAIDA_SQL = r'G:\Meu Drive\Indra company\SECRETARIA_DE_SAUDE_RECIFE\CAPS\GerarSQL_v2\insert_db'
SUFIXO_LAYOUT = '_layout.txt'
COLUNA_VERSION_FK = 'version_id'
CODIFICACAO_ARQUIVOS_DADOS = 'latin-1'
ARQUIVOS_RECONSTRUIR_LINHA = ['tb_componente_rede.txt']
ORDEM_PROCESSAMENTO_TABELAS = [
    'tb_detalhe', 'tb_ocupacao', 'tb_financiamento', 'tb_habilitacao',
    'tb_tipo_leito', 'tb_grupo', 'tb_registro', 'tb_rede_atencao',
    'tb_renases', 'tb_regra_condicionada', 'tb_rubrica',
    'tb_cid', 'tb_modalidade', 'tb_grupo_habilitacao', 'tb_servico', 'tb_tuss',
    'tb_sub_grupo', 'tb_componente_rede', 'tb_servico_classificacao',
    'tb_descricao_detalhe', 'tb_forma_organizacao', 'tb_procedimento',
    'tb_descricao', 'rl_procedimento_sia_sih', 'tb_sia_sih',
    'rl_procedimento_habilitacao', 'rl_procedimento_renases',
    'rl_procedimento_tuss', 'rl_procedimento_comp_rede',
    'rl_procedimento_cid', 'rl_procedimento_modalidade', 'rl_procedimento_ocupacao',
    'rl_procedimento_servico', 'rl_procedimento_leito', 'rl_procedimento_detalhe',
    'rl_excecao_compatibilidade', 'rl_procedimento_registro',
    'rl_procedimento_compativel', 'rl_procedimento_regra_cond',
    'rl_procedimento_origem', 'rl_procedimento_incremento'
]
DB_CONFIG = {
    'host': os.environ.get('DB_HOST_SIGTAP', 'localhost'),
    'port': os.environ.get('DB_PORT_SIGTAP', '5432'),
    'dbname': os.environ.get('DB_NAME_SIGTAP', 'postgres_sigtap07'), 
    'user': os.environ.get('DB_USER_SIGTAP', 'postgres'),
    'password': os.environ.get('DB_PASSWORD_SIGTAP', 'root')
}

# --- FUNÇÕES AUXILIARES ---

def conectar_bd(db_params):
    # (Implementação sem alterações)
    try:
        conn_params = db_params.copy()
        conn_params['client_encoding'] = 'utf8'
        conn = psycopg2.connect(**conn_params)
        return conn
    except psycopg2.Error as e:
        print(f"ERRO CRÍTICO ao conectar ao PostgreSQL: {e}")
        return None

def extrair_competencia(caminho_pasta_dados):
    # (Implementação sem alterações)
    nome_base_pasta = os.path.basename(caminho_pasta_dados)
    match = re.search(r'_(\d{6})_v', nome_base_pasta)
    if match:
        return match.group(1)
    else:
        print(f"AVISO: Não foi possível extrair competência de '{nome_base_pasta}'.")
        return None

def run_version_creation_step(conn, competencia):
    # (Implementação sem alterações)
    print("\n--- FASE 1: CRIAÇÃO DO REGISTRO DE VERSÃO ---")
    if not competencia:
        print("ERRO: Competência não pôde ser extraída. Impossível criar registro de versão.")
        return None, None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(id) FROM public.tb_version;")
            last_id = cur.fetchone()[0]
            next_id = (last_id or 0) + 1
            print(f"Último ID em tb_version: {last_id}. Novo ID será: {next_id}")
            cur.execute("SELECT id FROM public.tb_version WHERE competencia = %s", (competencia,))
            if cur.fetchone():
                print(f"ERRO: A competência '{competencia}' já existe no banco. Geração de inserts abortada para evitar duplicidade.")
                return None, None
            print(f"Criando novo registro com ID {next_id} para competência '{competencia}'...")
            insert_query = "INSERT INTO public.tb_version (id, competencia, created_at) VALUES (%s, %s, %s);"
            cur.execute(insert_query, (next_id, competencia, datetime.datetime.now()))
            conn.commit()
            print(f"SUCESSO! Registro criado. Version ID: {next_id}, Competência: {competencia}")
            return next_id, competencia
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERRO de banco de dados na FASE 1: {e}")
        return None, None

def parse_layout(layout_filepath):
    # (Implementação sem alterações)
    colunas_layout = []
    try:
        with open(layout_filepath, 'r', encoding=CODIFICACAO_ARQUIVOS_DADOS) as f:
            reader = csv.reader(f); next(reader)
            for i, row in enumerate(reader):
                if len(row) >= 5:
                    nome_coluna = row[0].strip().lower().replace(' ', '_').replace('-', '_')
                    try:
                        inicio, fim = int(row[2]) - 1, int(row[3])
                        colunas_layout.append({'nome': nome_coluna, 'inicio': inicio, 'fim': fim})
                    except (ValueError, IndexError):
                        print(f"Aviso: Pulando linha de layout inválida: {row}")
    except FileNotFoundError:
        return None
    return colunas_layout


def gerar_sql_para_arquivo_dados(data_filepath, layout_filepath, caminho_saida_sql, tabela_nome_base, version_id):
    colunas_layout = parse_layout(layout_filepath)
    if not colunas_layout: return False

    nome_arquivo_sql = f"{tabela_nome_base}_insert.sql"
    caminho_completo_sql = os.path.join(caminho_saida_sql, nome_arquivo_sql)
    colunas_para_insert = [col['nome'] for col in colunas_layout] + [COLUNA_VERSION_FK]
    colunas_sql_str = ", ".join([f'"{c}"' for c in colunas_para_insert])
    table_name_sql = f"public.\"{tabela_nome_base}\""

    try:
        with open(data_filepath, 'r', encoding=CODIFICACAO_ARQUIVOS_DADOS) as f_dados, \
             open(caminho_completo_sql, 'w', encoding='utf-8') as f_sql:
            
            print(f"  -> Gerando: {nome_arquivo_sql}")
            f_sql.write(f"-- INSERTS para a tabela {table_name_sql} com version_id = {version_id}\n\n")
            
            linhas_geradas = 0
            for linha_raw in f_dados:
                linha = linha_raw.rstrip('\n')
                if os.path.basename(data_filepath) in ARQUIVOS_RECONSTRUIR_LINHA and len(linha) < 206:
                    try: linha += next(f_dados).rstrip('\n')
                    except StopIteration: pass
                
                # --- ALTERAÇÃO APLICADA AQUI ---
                # Lógica para tratar valores vazios como NULL
                valores_formatados = []
                for col in colunas_layout:
                    valor_raw = linha[col['inicio']:col['fim']].strip()
                    if valor_raw == '':
                        valores_formatados.append("NULL")
                    else:
                        # Escapa aspas simples para não quebrar o SQL
                        valor_formatado = valor_raw.replace("'", "''")
                        valores_formatados.append(f"'{valor_formatado}'")
                
                valores_formatados.append(str(version_id))
                # --- FIM DA ALTERAÇÃO ---

                f_sql.write(f"INSERT INTO {table_name_sql} ({colunas_sql_str}) VALUES ({', '.join(valores_formatados)});\n")
                linhas_geradas += 1
            print(f"    {linhas_geradas} linhas de INSERT geradas.")
        return True
    except Exception as e:
        print(f"ERRO ao gerar SQL para {os.path.basename(data_filepath)}: {e}")
        return False

def run_sql_generation_step(version_id, competencia):
    # (Implementação sem alterações)
    print("\n--- FASE 2: GERAÇÃO DOS SCRIPTS SQL DE INSERT ---")
    print(f"Usando Version ID: {version_id} e Competência: {competencia}")
    data_hoje_str = datetime.date.today().strftime('%Y%m%d')
    nome_pasta_saida = f"insert_{data_hoje_str}_versaoID_{version_id}_competencia_{competencia}"
    caminho_pasta_saida_final = os.path.join(CAMINHO_BASE_SAIDA_SQL, nome_pasta_saida)
    os.makedirs(caminho_pasta_saida_final, exist_ok=True)
    print(f"Pasta de saída: {caminho_pasta_saida_final}")
    sucessos, erros = 0, []
    for nome_tabela in ORDEM_PROCESSAMENTO_TABELAS:
        data_file = os.path.join(CAMINHO_PASTA_DADOS, f"{nome_tabela}.txt")
        layout_file = os.path.join(CAMINHO_PASTA_DADOS, f"{nome_tabela}{SUFIXO_LAYOUT}")
        if os.path.exists(data_file) and os.path.exists(layout_file):
            print(f"Processando tabela: {nome_tabela}...")
            if gerar_sql_para_arquivo_dados(data_file, layout_file, caminho_pasta_saida_final, nome_tabela, version_id):
                sucessos += 1
            else:
                erros.append(nome_tabela)
    print("\n--- Resumo da Geração de Scripts ---")
    print(f"Arquivos processados com sucesso: {sucessos}")
    if erros:
        print(f"Tabelas com erro no processamento: {', '.join(erros)}")
    print(f"Verifique os arquivos SQL em: {caminho_pasta_saida_final}")

def main():
    # (Implementação sem alterações)
    print("--- INICIANDO SCRIPT UNIFICADO DE GERAÇÃO DE INSERTS (v1.1.0) ---")
    if not os.path.isdir(CAMINHO_PASTA_DADOS):
        print(f"ERRO CRÍTICO: CAMINHO_PASTA_DADOS '{CAMINHO_PASTA_DADOS}' não encontrado.")
        return
    if not os.path.isdir(CAMINHO_BASE_SAIDA_SQL):
        print(f"ERRO CRÍTICO: CAMINHO_BASE_SAIDA_SQL '{CAMINHO_BASE_SAIDA_SQL}' não encontrado.")
        return
    competencia_atual = extrair_competencia(CAMINHO_PASTA_DADOS)
    if not competencia_atual:
        print("Geração abortada. Não foi possível determinar a competência.")
        return
    conn = conectar_bd(DB_CONFIG)
    if not conn:
        return
    version_id, competencia = None, None
    try:
        version_id, competencia = run_version_creation_step(conn, competencia_atual)
    finally:
        if conn:
            conn.close()
            print("Conexão com o banco (FASE 1) fechada.")
    if not version_id:
        print("\nProcesso encerrado devido a falha na criação do registro de versão.")
        return
    run_sql_generation_step(version_id, competencia)
    print("\n--- PROCESSO UNIFICADO FINALIZADO ---")

if __name__ == '__main__':
    main()