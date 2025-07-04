# Nome do arquivo: executor_scripts_sql.py
# Versao do Arquivo 4.4.4 (Trata arquivos SQL vazios, mantém log em subpasta)
# Descrição: Esse arquivo faz a inserção dos dados nas tabelas,
#            com seleção interativa da pasta de scripts e ordem de arquivos atualizada.
import os
import datetime
import psycopg2
import psycopg2.errors 
from psycopg2 import sql 
import re

# --- Configurações do Banco de Dados ---
DB_HOST = os.environ.get('DB_HOST_SIGTAP', 'localhost')
DB_NAME = os.environ.get('DB_NAME_SIGTAP', 'postgres_sigtap07')
DB_USER = os.environ.get('DB_USER_SIGTAP', 'postgres')
DB_PASSWORD = os.environ.get('DB_PASSWORD_SIGTAP', 'root') 
DB_PORT = os.environ.get('DB_PORT_SIGTAP', '5432')

# --- Configurações Gerais do Script ---
CAMINHO_BASE_PASTAS_SQL = os.environ.get('SIGTAP_CAMINHO_BASE_SAIDA_SQL', r'G:\Meu Drive\Indra company\SECRETARIA_DE_SAUDE_RECIFE\CAPS\GerarSQL_v2\insert_db')
CODIFICACAO_ARQUIVOS_SQL = 'utf-8' 

COLUNA_VERSION_FK_EM_TABELAS = 'version_id'
TABELA_VERSION_NOME = 'tb_version'

ORDEM_PROCESSAMENTO_TABELAS_BASE = [
    'tb_detalhe', 'tb_ocupacao', 'tb_financiamento', 'tb_habilitacao',
    'tb_tipo_leito', 'tb_grupo', 'tb_registro', 'tb_rede_atencao',
    'tb_renases', 'tb_regra_condicionada', 'tb_rubrica',
    'tb_cid', 'tb_modalidade', 'tb_grupo_habilitacao', 'tb_servico', 'tb_tuss',
    'tb_sub_grupo', 'tb_componente_rede', 'tb_servico_classificacao',
    'tb_descricao_detalhe', 'tb_forma_organizacao', 'tb_procedimento',
    'tb_descricao', 
    'rl_procedimento_sia_sih',
    'tb_sia_sih',
    'rl_procedimento_habilitacao', 'rl_procedimento_renases',
    'rl_procedimento_tuss', 'rl_procedimento_comp_rede',
    'rl_procedimento_cid', 'rl_procedimento_modalidade', 'rl_procedimento_ocupacao',
    'rl_procedimento_servico', 'rl_procedimento_leito', 'rl_procedimento_detalhe',
    'rl_excecao_compatibilidade', 'rl_procedimento_registro',
    'rl_procedimento_compativel', 'rl_procedimento_regra_cond',
    'rl_procedimento_origem', 'rl_procedimento_incremento'
]
ORDEM_PROCESSAMENTO_ARQUIVOS_SQL = [f"{tabela_base}_insert.sql" for tabela_base in ORDEM_PROCESSAMENTO_TABELAS_BASE]

# (Funções conectar_bd, obter_info_carga_do_banco, solicitar_nome_manual, 
#  obter_pasta_scripts_sql, verificar_dados_existentes permanecem idênticas à v4.4.3)
# (A função executar_scripts_sql_da_pasta terá a modificação para arquivos vazios)

def conectar_bd():
    """Estabelece conexão com o banco de dados."""
    conn_params = {
        'host': DB_HOST,
        'dbname': DB_NAME,
        'user': DB_USER,
        'password': DB_PASSWORD,
        'port': DB_PORT,
        'client_encoding': 'utf8'
    }
    try:
        conn = psycopg2.connect(**conn_params)
        print(f"Conectado ao banco de dados '{DB_NAME}' em '{DB_HOST}'.")
        return conn
    except psycopg2.Error as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}")
        return None

def obter_info_carga_do_banco(conn, version_id_da_pasta, competencia_da_pasta_nome):
    """Obtém a competência real do banco para o version_id da pasta."""
    if not conn or not version_id_da_pasta:
        print("AVISO: Não foi possível obter informações da carga do banco (sem conexão ou version_id_da_pasta).")
        return None, None 

    try:
        with conn.cursor() as cur:
            query = sql.SQL("SELECT competencia FROM public.{table_name} WHERE id = %s").format(
                table_name=sql.Identifier(TABELA_VERSION_NOME)
            )
            cur.execute(query, (version_id_da_pasta,))
            result = cur.fetchone()
            if result:
                competencia_banco = result[0]
                print(f"  Verificação no DB: Version ID {version_id_da_pasta} corresponde à competência {competencia_banco}.")
                if competencia_da_pasta_nome and competencia_banco != competencia_da_pasta_nome:
                    print(f"  ALERTA: Competência extraída do nome da pasta ({competencia_da_pasta_nome}) diverge da competência no DB ({competencia_banco}) para o Version ID {version_id_da_pasta}.")
                return version_id_da_pasta, competencia_banco
            else:
                print(f"  ERRO: Version ID {version_id_da_pasta} (extraído/informado da pasta) não encontrado na tabela '{TABELA_VERSION_NOME}'.")
                return None, None
    except psycopg2.Error as e:
        print(f"Erro ao consultar {TABELA_VERSION_NOME} para obter informações da carga: {e}")
        return None, None

def solicitar_nome_manual(caminho_base):
    """Função auxiliar para solicitar o nome da pasta manualmente."""
    while True:
        print(f"\nAs pastas de script devem estar em: {caminho_base}")
        print("Exemplo de nome de pasta esperado: insert_AAAAMMDD_versaoID_X_competencia_YYYYMM")
        nome_pasta_completo = input("Digite o nome COMPLETO da pasta de scripts SQL a ser executada: ").strip()
        
        if not nome_pasta_completo:
            print("Nome da pasta não pode ser vazio.")
            continue

        caminho_completo_pasta = os.path.join(caminho_base, nome_pasta_completo)
        if os.path.isdir(caminho_completo_pasta):
            data_para_log = nome_pasta_completo 
            match = re.fullmatch(r"insert_(\d{8})_versaoID_(\d+)_competencia_(\d{6})", nome_pasta_completo)
            if match:
                data_para_log = match.group(1) 
            else:
                print(f"AVISO: O nome da pasta '{nome_pasta_completo}' não segue o padrão 'insert_AAAAMMDD_versaoID_X_competencia_YYYYMM'.")
                match_antigo_data_hoje = re.match(r"insert_(\d{8})", nome_pasta_completo) 
                if match_antigo_data_hoje:
                    data_para_log = match_antigo_data_hoje.group(1)
                else: 
                    data_para_log = datetime.datetime.now().strftime('%Y%m%d')
            return caminho_completo_pasta, nome_pasta_completo, data_para_log
        else:
            print(f"Pasta '{caminho_completo_pasta}' não encontrada. Verifique o nome digitado.")

def obter_pasta_scripts_sql(caminho_base):
    """Lista pastas de script no formato esperado e permite ao usuário escolher."""
    print(f"\n--- Seleção da Pasta de Scripts SQL ---")
    print(f"Procurando pastas em: {caminho_base}")
    pastas_validas = []
    try:
        if not os.path.isdir(caminho_base):
            print(f"ERRO: O caminho base para pastas SQL '{caminho_base}' não existe ou não é um diretório.")
            return None, None, None
        for nome_item in os.listdir(caminho_base):
            caminho_item = os.path.join(caminho_base, nome_item)
            if os.path.isdir(caminho_item):
                match = re.fullmatch(r"insert_(\d{8})_versaoID_(\d+)_competencia_(\d{6})", nome_item)
                if match:
                    data_str = match.group(1); version_id_str = match.group(2); competencia_str = match.group(3)
                    try:
                        data_obj = datetime.datetime.strptime(data_str, "%Y%m%d")
                        pastas_validas.append({"nome_completo": nome_item, "caminho_completo": caminho_item, "data_obj": data_obj,
                                             "data_str_aaaammdd": data_str, "version_id": version_id_str, "competencia": competencia_str})
                    except ValueError: print(f"Aviso: Pasta '{nome_item}' data ('{data_str}') inválida, pulando.")
    except Exception as e: print(f"Erro ao listar pastas: {e}"); return None, None, None
    if not pastas_validas:
        print(f"\nNenhuma pasta no formato esperado (insert_AAAAMMDD_versaoID_X_competencia_YYYYMM) encontrada em '{caminho_base}'.")
        return solicitar_nome_manual(caminho_base)
    pastas_validas.sort(key=lambda p: (p["data_obj"], p["competencia"], int(p["version_id"])), reverse=True)
    print("\nPastas de script encontradas:")
    for i, p_info in enumerate(pastas_validas):
        print(f"  {i + 1}: {p_info['nome_completo']} (Data: {p_info['data_obj']:%d/%m/%Y}, Versão ID: {p_info['version_id']}, Competência: {p_info['competencia']})")
    print("  0: Digitar nome da pasta manualmente")
    while True:
        try:
            esc_str = input(f"Digite o número da pasta (1-{len(pastas_validas)}) ou 0 para manual: ").strip()
            if not esc_str: continue
            esc_num = int(esc_str)
            if esc_num == 0: return solicitar_nome_manual(caminho_base)
            if 1 <= esc_num <= len(pastas_validas):
                p_sel = pastas_validas[esc_num - 1]
                print(f"Pasta selecionada: {p_sel['nome_completo']}")
                return p_sel["caminho_completo"], p_sel["nome_completo"], p_sel["data_str_aaaammdd"]
            else: print(f"Escolha inválida. Digite um número entre 0 e {len(pastas_validas)}.")
        except ValueError: print("Entrada inválida. Digite um número.")
        except Exception as e: print(f"Erro na seleção: {e}"); return None, None, None

def verificar_dados_existentes(conn, tabela, version_id):
    try:
        with conn.cursor() as cur:
            t_parts = tabela.split('.'); schema = sql.Identifier(t_parts[0]) if len(t_parts) > 1 else sql.Identifier('public'); table = sql.Identifier(t_parts[-1])
            t_sql_obj = sql.SQL("{}.{}").format(schema, table) if len(t_parts) > 1 else sql.SQL("public.{}").format(table)
            query = sql.SQL("SELECT 1 FROM {table} WHERE {version_col} = %s LIMIT 1").format(
                table=t_sql_obj, version_col=sql.Identifier(COLUNA_VERSION_FK_EM_TABELAS))
            cur.execute(query, (version_id,)); return cur.fetchone() is not None
    except psycopg2.errors.UndefinedTable: print(f"  Aviso: Tabela '{tabela}' não encontrada (será criada?)."); return False 
    except psycopg2.Error as e: print(f"Erro DB ao verificar dados existentes para {tabela} vID {version_id}: {e.pgcode if hasattr(e, 'pgcode') else ''} - {e.pgerror if hasattr(e, 'pgerror') else str(e)}"); return True 
    except Exception as e_gen: print(f"Erro INESPERADO ao verificar dados para {tabela} ({type(e_gen).__name__}): {e_gen}"); import traceback; traceback.print_exc(); return True

def executar_scripts_sql_da_pasta(conn, caminho_pasta_scripts, version_id_da_pasta, competencia_da_pasta_db):
    if not os.path.isdir(caminho_pasta_scripts):
        print(f"Erro: Caminho da pasta '{caminho_pasta_scripts}' não é um diretório."); return [], [], [], []
    print(f"\nExecutando scripts SQL da pasta: {caminho_pasta_scripts}")
    print(f"Associado ao Version ID: {version_id_da_pasta}, Competência no DB: {competencia_da_pasta_db}")
    scripts_a_executar = []; arquivos_na_pasta = set(os.listdir(caminho_pasta_scripts))
    for nome_arq_esp in ORDEM_PROCESSAMENTO_ARQUIVOS_SQL:
        if nome_arq_esp in arquivos_na_pasta:
            scripts_a_executar.append(nome_arq_esp); arquivos_na_pasta.remove(nome_arq_esp) 
        else: print(f"  Aviso: Arquivo SQL esperado '{nome_arq_esp}' não encontrado na pasta.")
    if arquivos_na_pasta:
        extras = sorted([f for f in arquivos_na_pasta if f.endswith(".sql")])
        if extras: print(f"  Aviso: Arquivos .sql adicionais encontrados (serão executados ao final): {', '.join(extras)}"); scripts_a_executar.extend(extras)
    
    sucesso_lista, falha_lista, pulados_lista, erros_lista = [], [], [], []
    for nome_arquivo_sql in scripts_a_executar:
        caminho_script_sql = os.path.join(caminho_pasta_scripts, nome_arquivo_sql)
        tabela_alvo_base = nome_arquivo_sql.replace("_insert.sql", ""); tabela_alvo_schema = f"public.{tabela_alvo_base}"
        print(f"\nProcessando script: {nome_arquivo_sql} (Tabela Alvo: {tabela_alvo_schema})")
        if verificar_dados_existentes(conn, tabela_alvo_schema, version_id_da_pasta):
            print(f"  AVISO: Dados já existem para '{tabela_alvo_schema}' com version_id {version_id_da_pasta}. Script pulado."); pulados_lista.append(nome_arquivo_sql); conn.rollback(); continue
        try:
            with open(caminho_script_sql, 'r', encoding=CODIFICACAO_ARQUIVOS_SQL) as f_script:
                sql_content = f_script.read().strip() 
            if not sql_content: # MODIFICAÇÃO AQUI
                print(f"  AVISO: Arquivo SQL '{nome_arquivo_sql}' está vazio ou contém apenas comentários. Pulando execução.")
                pulados_lista.append(f"{nome_arquivo_sql} (vazio)") # Adiciona à lista de pulados com motivo
                continue
            with conn.cursor() as cur:
                print(f"  Executando {nome_arquivo_sql}..."); cur.execute(sql_content)
            conn.commit(); print(f"  Script {nome_arquivo_sql} executado com sucesso."); sucesso_lista.append(nome_arquivo_sql)
        except psycopg2.Error as e_script:
            conn.rollback()
            err_detail = {'script': nome_arquivo_sql, 'tabela_alvo': tabela_alvo_schema, 'error_type': type(e_script).__name__,
                          'pgcode': e_script.pgcode if hasattr(e_script, 'pgcode') else 'N/A', 
                          'pgerror': str(e_script.pgerror).strip() if hasattr(e_script, 'pgerror') and e_script.pgerror else "N/A",
                          'diag_message_primary': e_script.diag.message_primary if hasattr(e_script, 'diag') else "N/A",
                          'diag_message_detail': e_script.diag.message_detail if hasattr(e_script, 'diag') else "N/A",
                          'full_error': str(e_script)}
            erros_lista.append(err_detail); falha_lista.append(nome_arquivo_sql)
            print(f"  ERRO ao executar {nome_arquivo_sql}: {err_detail['diag_message_primary'] or err_detail['pgerror'] or err_detail['full_error']}")
            print(f"  Transação para {nome_arquivo_sql} revertida.")
        except FileNotFoundError: print(f"  ERRO: Arquivo {nome_arquivo_sql} não encontrado em {caminho_script_sql}.")
        except Exception as e_geral:
            conn.rollback() 
            err_detail = {'script': nome_arquivo_sql, 'tabela_alvo': tabela_alvo_schema, 'error_type': type(e_geral).__name__, 'full_error': str(e_geral)}
            erros_lista.append(err_detail); falha_lista.append(nome_arquivo_sql)
            print(f"  ERRO INESPERADO ao processar {nome_arquivo_sql}: {e_geral}\n  Transação revertida.")
    return sucesso_lista, falha_lista, pulados_lista, erros_lista

def main():
    print(f"--- Executor de Scripts SQL SIGTAP v4.4.4 ---")
    conn = conectar_bd(); 
    if not conn: return

    caminho_scripts, nome_pasta_sel, data_log_str = obter_pasta_scripts_sql(CAMINHO_BASE_PASTAS_SQL)
    if not caminho_scripts: print("Nenhuma pasta selecionada. Saindo."); conn.close(); return

    v_id_pasta, comp_pasta_nome = None, None
    match_info = re.fullmatch(r"insert_(\d{8})_versaoID_(\d+)_competencia_(\d{6})", nome_pasta_sel)
    if match_info:
        v_id_pasta = match_info.group(2); comp_pasta_nome = match_info.group(3)
        print(f"\nInfos da pasta '{nome_pasta_sel}': Data={data_log_str}, VersionID={v_id_pasta}, Competência={comp_pasta_nome}")
    else:
        print(f"\nAVISO: Não extraí VersionID/Competência de '{nome_pasta_sel}'.")
        v_id_pasta = input("  Digite o Version ID para esta carga: ").strip()
        if not (v_id_pasta and v_id_pasta.isdigit()): print("Version ID inválido. Abortando."); conn.close(); return
    
    v_id_db, comp_db = obter_info_carga_do_banco(conn, v_id_pasta, comp_pasta_nome)
    if not v_id_db: print(f"ERRO: Version ID '{v_id_pasta}' não confirmado no DB. Abortando."); conn.close(); return
    
    print(f"\nPronto para executar scripts de '{nome_pasta_sel}'")
    print(f"Version ID (pasta): {v_id_pasta}, Competência no DB (para este vID): {comp_db or 'N/A'}")
    if not input("Continuar? (s/N): ").lower() == 's': print("Cancelado."); conn.close(); return

    sucesso, falha, pulados, erros = executar_scripts_sql_da_pasta(conn, caminho_scripts, v_id_pasta, comp_db)
    
    print("\n--- Resumo da Execução ---"); print(f"Pasta: {nome_pasta_sel}\nVersion ID Carga: {v_id_pasta} (Competência DB: {comp_db or 'N/A'})")
    if sucesso: print(f"\n{len(sucesso)} Script(s) com SUCESSO:\n  - " + "\n  - ".join(sucesso))
    if pulados: print(f"\n{len(pulados)} Script(s) PULADO(S):\n  - " + "\n  - ".join(pulados))
    if falha: print(f"\n{len(falha)} Script(s) com FALHA:\n  - " + "\n  - ".join(falha))
    if not any([sucesso, falha, pulados]): print("\nNenhum script processado ou qualificado.")
    if erros:
        log_dir = os.path.join(CAMINHO_BASE_PASTAS_SQL, "log_erro")
        try: os.makedirs(log_dir, exist_ok=True)
        except OSError as e: print(f"AVISO: Falha ao criar dir de log '{log_dir}': {e}. Log em '{CAMINHO_BASE_PASTAS_SQL}'."); log_dir = CAMINHO_BASE_PASTAS_SQL
        log_fn = f"log_erros_execucao_{data_log_str}_vID{v_id_pasta}_{datetime.datetime.now():%Y%m%d_%H%M%S}.txt"
        log_path = os.path.join(log_dir, log_fn)
        print(f"\n{len(erros)} erro(s) detalhado(s). Log: {log_path}")
        try:
            with open(log_path, 'w', encoding=CODIFICACAO_ARQUIVOS_SQL) as f_log:
                f_log.write(f"Log de Erros - Execução Scripts SQL\nPasta: {nome_pasta_sel}\nCaminho: {caminho_scripts}\nData Exec.: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}\n")
                f_log.write(f"Version ID (pasta): {v_id_pasta}\nCompetência (DB p/ vID): {comp_db or 'N/A'}\n\n")
                for i, err_info in enumerate(erros):
                    f_log.write(f"--- Erro {i+1} ---\n" + "\n".join([f"{k}: {v}" for k, v in err_info.items()]) + "\n\n")
        except IOError as e_io: print(f"ERRO ao escrever log '{log_path}': {e_io}")
    if conn: conn.close(); print("\nConexão com o banco fechada.")

if __name__ == '__main__':
    main()