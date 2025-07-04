# Nome do arquivo: executor_direto_db.py
#Descrição: Este script é um carregador de dados direto para o banco de dados. Em vez de primeiro criar arquivos .sql e depois executá-los, ele lê os dados brutos (.txt) e os insere diretamente no seu banco de dados PostgreSQL. É uma abordagem mais integrada e eficiente.
import os
import csv
import datetime
import psycopg2 # Para interagir com PostgreSQL

# --- Configurações do Banco de Dados ---
DB_HOST = os.environ.get('DB_HOST_SIGTAP', 'localhost')
DB_NAME = os.environ.get('DB_NAME_SIGTAP', 'sigtap')
DB_USER = os.environ.get('DB_USER_SIGTAP', 'postgres')
DB_PASSWORD = os.environ.get('DB_PASSWORD_SIGTAP', 'root')
DB_PORT = os.environ.get('DB_PORT_SIGTAP', '5432')

# --- Configurações Gerais do Script ---
CAMINHO_PASTA_DADOS = r'G:\Meu Drive\Indra company\SECRETARIA_DE_SAUDE_RECIFE\CAPS\TabelaUnificada_202504_v2504031832'

SUFIXO_LAYOUT = '_layout.txt'
COLUNA_VERSION_FK = 'version_id'

# ORDEM DE PROCESSAMENTO AJUSTADA:
ORDEM_PROCESSAMENTO_TABELAS = [
    'tb_detalhe', 'tb_ocupacao', 'tb_financiamento', 'tb_habilitacao',
    'tb_tipo_leito', 'tb_grupo', 'tb_registro', 'tb_rede_atencao',
    'tb_renases', 'tb_regra_condicionada', 'tb_rubrica',
    'tb_cid', 'tb_modalidade', 'tb_grupo_habilitacao', 'tb_servico', 'tb_tuss',
    'tb_sub_grupo', 'tb_componente_rede', 'tb_servico_classificacao',
    'tb_descricao_detalhe', 'tb_forma_organizacao', 'tb_procedimento',
    'tb_descricao', 
    'rl_procedimento_sia_sih', # Movida para antes de tb_sia_sih
    'tb_sia_sih',             # Movida para depois de rl_procedimento_sia_sih
    'rl_procedimento_habilitacao', 'rl_procedimento_renases',
    'rl_procedimento_tuss', 'rl_procedimento_comp_rede',
    'rl_procedimento_cid', 'rl_procedimento_modalidade', 'rl_procedimento_ocupacao',
    'rl_procedimento_servico', 'rl_procedimento_leito', 'rl_procedimento_detalhe',
    'rl_excecao_compatibilidade', 'rl_procedimento_registro',
    'rl_procedimento_compativel', 'rl_procedimento_regra_cond',
    'rl_procedimento_origem', 'rl_procedimento_incremento'
]

# CODIFICAÇÃO ESPERADA DOS ARQUIVOS DE DADOS:
# Mude para 'cp1252' se 'latin-1' não funcionar.
# É crucial que esta seja a codificação REAL dos seus arquivos .txt
CODIFICACAO_ARQUIVOS_DADOS = 'latin-1'


# --- Funções Auxiliares ---
def connect_db():
    conn_string = f"host='{DB_HOST}' dbname='{DB_NAME}' user='{DB_USER}' password='{DB_PASSWORD}' port='{DB_PORT}'"
    try:
        conn = psycopg2.connect(conn_string)
        print(f"Conectado ao banco de dados '{DB_NAME}' em '{DB_HOST}'.")
        return conn
    except psycopg2.Error as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}")
        return None

def parse_layout_file(layout_filepath):
    column_definitions = []
    try:
        # Layouts também podem ter problemas de encoding se contiverem caracteres especiais nos nomes das colunas ou tipos
        # mas geralmente são mais simples. Se houver erro aqui, pode ser necessário ajustar encoding também.
        with open(layout_filepath, 'r', encoding=CODIFICACAO_ARQUIVOS_DADOS) as f: # Tentar mesma codificação para layout
            reader = csv.reader(f)
            header = next(reader)
            header_map = {h.strip().lower(): i for i, h in enumerate(header)}
            required_cols = ['coluna', 'tamanho', 'inicio', 'fim', 'tipo']
            if not all(col in header_map for col in required_cols):
                print(f"Erro: Layout {os.path.basename(layout_filepath)} sem colunas: {', '.join(required_cols)}.")
                return None
            for i, row in enumerate(reader):
                if not row or len(row) < len(required_cols):
                    continue
                try:
                    col_name = row[header_map['coluna']].strip().lower()
                    inicio = int(row[header_map['inicio']].strip())
                    fim = int(row[header_map['fim']].strip())
                    col_type_from_layout = row[header_map['tipo']].strip().upper()
                    if not col_name or inicio <= 0 or fim < inicio:
                        continue
                    column_definitions.append({
                        'coluna_db': col_name,
                        'inicio': inicio,
                        'fim': fim,
                        'tipo_layout': col_type_from_layout
                    })
                except (IndexError, ValueError):
                    continue
        return column_definitions
    except UnicodeDecodeError as ude:
        print(f"Erro de encoding ao ler arquivo de layout {layout_filepath} com {CODIFICACAO_ARQUIVOS_DADOS}: {ude}")
        print("Tente ajustar a CODIFICACAO_ARQUIVOS_DADOS no script ou verifique a codificação do arquivo de layout.")
        return None
    except FileNotFoundError:
        return None
    except Exception as e:
        print(f"Erro ao ler/parsear layout {layout_filepath}: {e}")
        return None

def prepare_value_for_db(value_str, tipo_layout):
    if value_str is None or value_str.strip() == '':
        return None
    cleaned_value = value_str.strip()

    tipos_inteiros = ['NUM', 'INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'QT_PONTOS', 'QTDE', 
                      'VL_IDADE_MINIMA', 'VL_IDADE_MAXIMA', 'ID']
    codigos_inteiros = ['CO_REGISTRO', 'CO_MODALIDADE', 'CO_GRUPO', 'CO_SUB_GRUPO', 
                        'CO_FORMA_ORGANIZACAO', 'CO_FINANCIAMENTO', 'CO_DETALHE', 
                        'CO_SERVICO', 'CO_CLASSIFICACAO', 'CO_TIPO_LEITO', 'CO_REGRA_CONDICIONADA',
                        'NU_GRUPO_HABILITACAO'] 
    tipos_decimais = ['DEC', 'DECIMAL', 'FLOAT', 'DOUBLE', 'NUMBER', 'VL_SH', 'VL_SA', 'VL_SP', 
                      'VL_PERCENTUAL_SH', 'VL_PERCENTUAL_SA', 'VL_PERCENTUAL_SP']

    if tipo_layout in tipos_inteiros or (tipo_layout.startswith('CO_') and tipo_layout in codigos_inteiros) or tipo_layout.startswith('NU_'):
        try:
            return int(cleaned_value)
        except ValueError:
            if cleaned_value: 
                 return cleaned_value # Pode ser um código alfanumérico
            return None
    elif tipo_layout in tipos_decimais:
        try:
            return float(cleaned_value.replace(',', '.'))
        except ValueError:
            return None
    elif tipo_layout in ['DATE', 'DT_COMPETENCIA', 'DT_INICIO_VIGENCIA', 'DT_FIM_VIGENCIA']:
        if len(cleaned_value) == 6: # YYYYMM para dt_competencia
            return cleaned_value # Se a coluna no DB for VARCHAR(6)
        # Adicione outras lógicas de conversão de data aqui se necessário
        # Ex: try: return datetime.datetime.strptime(cleaned_value, '%Y%m%d').date() except ValueError: pass
        return cleaned_value 
        
    return cleaned_value

# --- Lógica Principal ---
def main():
    print(f"--- Executor Direto de INSERTs no Banco de Dados (Codificação: {CODIFICACAO_ARQUIVOS_DADOS}) ---")
    version_id_input = input(f"Por favor, insira o valor para '{COLUNA_VERSION_FK}' (deve existir em tb_version): ").strip()
    if not version_id_input or not version_id_input.isdigit():
        print("Erro: version_id inválido. Deve ser um número.")
        return
    try:
        version_id_value = int(version_id_input)
    except ValueError:
        print("Erro: version_id deve ser um número inteiro.")
        return

    conn = connect_db()
    if not conn:
        return

    inserts_sucedidos_por_tabela = {tbl.lower(): 0 for tbl in ORDEM_PROCESSAMENTO_TABELAS}
    erros_insercao_detalhados = []
    tabelas_com_falha_commit = [] 

    try:
        if not os.path.isdir(CAMINHO_PASTA_DADOS):
            print(f"Erro: O caminho da pasta de dados não foi encontrado: {CAMINHO_PASTA_DADOS}")
            if conn: conn.close()
            return
            
        arquivos_de_dados_existentes = {}
        for f_name in os.listdir(CAMINHO_PASTA_DADOS):
            if f_name.lower().endswith('.txt') and not f_name.lower().endswith(SUFIXO_LAYOUT.lower()):
                base_name = f_name.rsplit('.', 1)[0].lower()
                arquivos_de_dados_existentes[base_name] = f_name

        arquivos_para_processar_ordenados = []
        nomes_base_encontrados = set()
        for nome_base_tabela_ordenada in ORDEM_PROCESSAMENTO_TABELAS:
            nome_base_lower = nome_base_tabela_ordenada.lower()
            if nome_base_lower in arquivos_de_dados_existentes:
                arquivos_para_processar_ordenados.append(arquivos_de_dados_existentes[nome_base_lower])
                nomes_base_encontrados.add(nome_base_lower)

        arquivos_nao_ordenados = []
        for base_name, original_filename in arquivos_de_dados_existentes.items():
            if base_name not in nomes_base_encontrados:
                arquivos_nao_ordenados.append(original_filename)
        
        if arquivos_nao_ordenados:
            print("\nAviso: Arquivos de dados encontrados sem ordem definida, serão processados ao final:")
            for fname in arquivos_nao_ordenados: print(f"- {fname}")
            arquivos_para_processar_ordenados.extend(arquivos_nao_ordenados)

        if not arquivos_para_processar_ordenados:
            print(f"Nenhum arquivo de dados encontrado para processar em {CAMINHO_PASTA_DADOS}.")
            if conn: conn.close()
            return
            
        print("\nIniciando processamento e inserção no banco de dados...")
        print(f"Usando {COLUNA_VERSION_FK}: {version_id_value}")
        print("-" * 40)

        with conn.cursor() as cursor:
            for data_filename in arquivos_para_processar_ordenados:
                base_name_original = data_filename.rsplit('.', 1)[0]
                table_name_db = base_name_original.lower()
                layout_filename = f"{base_name_original}{SUFIXO_LAYOUT}"
                data_filepath = os.path.join(CAMINHO_PASTA_DADOS, data_filename)
                layout_filepath = os.path.join(CAMINHO_PASTA_DADOS, layout_filename)

                print(f"\nProcessando arquivo: {data_filename} para tabela: {table_name_db}")

                if not os.path.exists(layout_filepath):
                    erros_insercao_detalhados.append({'tabela': table_name_db, 'tipo_erro': 'Configuração', 'detalhe': 'Arquivo de layout não encontrado.'})
                    continue

                column_definitions = parse_layout_file(layout_filepath)
                if not column_definitions:
                    erros_insercao_detalhados.append({'tabela': table_name_db, 'tipo_erro': 'Configuração', 'detalhe': 'Não foi possível parsear o arquivo de layout.'})
                    continue

                db_column_names = ['id', COLUNA_VERSION_FK.lower()] + [col['coluna_db'] for col in column_definitions]
                placeholders = ', '.join(['%s'] * len(db_column_names))
                sql_column_names_str = ", ".join(f'"{c.lower()}"' if not c.isidentifier() or c.upper() in ['GROUP', 'ORDER', 'PRIMARY', 'TABLE', 'USER'] else c.lower() for c in db_column_names)
                insert_sql_template = f"INSERT INTO {table_name_db} ({sql_column_names_str}) VALUES ({placeholders})"
                
                row_id_counter = 1
                linhas_inseridas_nesta_tabela_antes_commit = 0 
                erros_neste_arquivo = 0

                try:
                    # MUDANÇA AQUI: encoding
                    with open(data_filepath, 'r', encoding=CODIFICACAO_ARQUIVOS_DADOS) as infile:
                        for line_number, line_content_full in enumerate(infile, 1):
                            line_content_stripped = line_content_full.rstrip('\r\n')
                            if not line_content_stripped: continue

                            extracted_raw_values = []
                            valid_line = True
                            for col_def in column_definitions:
                                start_pos = col_def['inicio'] - 1
                                end_pos = col_def['fim']
                                if start_pos < 0 or end_pos > len(line_content_stripped) or start_pos >= end_pos:
                                    msg = f"Linha {line_number} curta/layout inválido para coluna '{col_def['coluna_db']}' (Tam: {len(line_content_stripped)}, Esperado até: {end_pos})."
                                    erros_insercao_detalhados.append({'tabela': table_name_db, 'id_linha_script': row_id_counter, 'linha_dados': line_content_stripped[:100], 'tipo_erro': 'Formato Dados', 'detalhe': msg})
                                    valid_line = False; erros_neste_arquivo += 1; break
                                extracted_raw_values.append(line_content_stripped[start_pos:end_pos])
                            
                            if not valid_line: continue

                            db_values = [row_id_counter, version_id_value]
                            temp_prepared_values = []
                            for raw_val, col_def in zip(extracted_raw_values, column_definitions):
                                prepared_val = prepare_value_for_db(raw_val, col_def['tipo_layout'])
                                temp_prepared_values.append(prepared_val)
                            db_values.extend(temp_prepared_values)
                            
                            try:
                                cursor.execute(insert_sql_template, tuple(db_values))
                                linhas_inseridas_nesta_tabela_antes_commit += 1 
                            except psycopg2.Error as db_err:
                                sql_tentado_debug = "SQL indisponível (erro no mogrify)"
                                try:
                                    sql_tentado_debug = cursor.mogrify(insert_sql_template, tuple(db_values)).decode(CODIFICACAO_ARQUIVOS_DADOS, errors='ignore')
                                except Exception: 
                                    pass 
                                erros_insercao_detalhados.append({
                                    'tabela': table_name_db, 'id_linha_script': row_id_counter, 
                                    'linha_dados': line_content_stripped[:100], 'tipo_erro': 'Banco de Dados', 
                                    'detalhe': str(db_err).strip(), 'sql_tentado': sql_tentado_debug
                                })
                                erros_neste_arquivo += 1
                            row_id_counter += 1
                    
                    if erros_neste_arquivo > 0:
                        conn.rollback()
                        tabelas_com_falha_commit.append(table_name_db)
                    else:
                        if linhas_inseridas_nesta_tabela_antes_commit > 0:
                            conn.commit()
                            inserts_sucedidos_por_tabela[table_name_db] = inserts_sucedidos_por_tabela.get(table_name_db, 0) + linhas_inseridas_nesta_tabela_antes_commit
                
                except UnicodeDecodeError as ude: # Captura erro de encoding ao ler o arquivo de dados
                    erros_insercao_detalhados.append({'tabela': table_name_db, 'tipo_erro': 'Encoding Dados', 'detalhe': f"Erro ao ler {data_filename} com {CODIFICACAO_ARQUIVOS_DADOS}: {ude}"})
                    tabelas_com_falha_commit.append(table_name_db) # Considera falha para esta tabela
                    try: conn.rollback() # Garante que nada da transação atual seja comitado
                    except psycopg2.Error: pass

                except FileNotFoundError:
                    erros_insercao_detalhados.append({'tabela': table_name_db, 'tipo_erro': 'Configuração', 'detalhe': 'Arquivo de dados não encontrado.'})
                except Exception as e_file:
                    try: conn.rollback() 
                    except psycopg2.Error: pass 
                    tabelas_com_falha_commit.append(table_name_db)
                    erros_insercao_detalhados.append({'tabela': table_name_db, 'tipo_erro': 'Processamento Arquivo', 'detalhe': str(e_file)})

    except psycopg2.Error as e:
        print(f"Erro geral de DB: {e}")
    except Exception as e_main:
        print(f"Erro inesperado na lógica principal: {e_main}")
        import traceback; traceback.print_exc()
    finally:
        if conn: conn.close(); print("\nConexão com o banco de dados fechada.")

    # --- Relatório Final ---
    print("\n--- Relatório Final da Execução ---")
    tabelas_vazias_final = []
    nomes_tabelas_relatorio = set(ord_tbl.lower() for ord_tbl in ORDEM_PROCESSAMENTO_TABELAS)
    nomes_tabelas_relatorio.update(arq_existente.rsplit('.',1)[0].lower() for arq_existente in arquivos_para_processar_ordenados)

    print("\nStatus de Inserção por Tabela (após commits/rollbacks):")
    for nome_tabela_base in sorted(list(nomes_tabelas_relatorio)):
        sucessos = inserts_sucedidos_por_tabela.get(nome_tabela_base, 0)
        status = ""
        if nome_tabela_base in tabelas_com_falha_commit:
            status = "COM ERROS (ROLLBACK ou Falha Leitura)"
        elif sucessos > 0:
            status = "OK"
        else: 
            arquivo_processado = any(nome_tabela_base == f.rsplit('.', 1)[0].lower() for f in arquivos_para_processar_ordenados)
            if arquivo_processado:
                 status = "VAZIA (sem dados no arquivo ou todos com erro pré-DB)"
            else: 
                 status = "NÃO PROCESSADA (arquivo de dados ausente ou não listado)"
        
        print(f"- Tabela: {nome_tabela_base:<30} Inserções Comitadas: {sucessos:<5} Status: {status}")
        if sucessos == 0 : 
            tabelas_vazias_final.append(f"{nome_tabela_base} (Status: {status})")

    if tabelas_vazias_final:
        print("\nAs seguintes tabelas não receberam dados (ou tiveram rollback total / erro pré-DB / arquivo ausente):")
        for nome_tabela_status in tabelas_vazias_final: print(f"  - {nome_tabela_status}")

    if erros_insercao_detalhados:
        print(f"\nTotal de {len(erros_insercao_detalhados)} erros detalhados:")
        log_erros_path = f"log_erros_insercao_direta_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(log_erros_path, 'w', encoding='utf-8') as f_log: # Sempre UTF-8 para o log
            f_log.write(f"Log de Erros de Inserção Direta - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f_log.write(f"Version ID usado: {version_id_value}\n")
            f_log.write(f"Codificação de leitura dos arquivos de dados: {CODIFICACAO_ARQUIVOS_DADOS}\n\n")
            for i, erro_info in enumerate(erros_insercao_detalhados):
                log_msg = f"{i+1}. Tabela: {erro_info.get('tabela', 'N/A')}\n"
                log_msg += f"   Tipo Erro: {erro_info.get('tipo_erro', 'Desconhecido')}\n"
                if 'id_linha_script' in erro_info: # Só mostra se for erro em uma linha específica
                    log_msg += f"   ID Linha (Script): {erro_info['id_linha_script']}\n"
                    log_msg += f"   Dados (início): {erro_info.get('linha_dados', 'N/A')}\n"
                log_msg += f"   Detalhe: {erro_info.get('detalhe', 'N/A')}\n"
                if 'sql_tentado' in erro_info and erro_info['sql_tentado']: 
                    log_msg += f"   SQL Tentado: {erro_info['sql_tentado']}\n"
                log_msg += "-------------------------------------\n"
                f_log.write(log_msg)
        print(f"\nDetalhes dos erros foram salvos em: {log_erros_path}")
    else:
        print("\nNenhum erro detalhado registrado.")
    print("--- Fim da Execução ---")

if __name__ == '__main__':
    main()