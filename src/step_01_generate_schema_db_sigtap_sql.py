# Versao do Arquivo: v2.8 (id BIGSERIAL universal, PK (id, version_id), UNIQUEs ajustadas conforme decisão)
# Descrição: Gerar o script para criação do banco de dados.
#            'id' como BIGSERIAL e PK (id, version_id) para todas as tabelas de dados.
#            UNIQUE constraints (co_X, version_id) são aplicadas SELETIVAMENTE.
# Nome do arquivo: Step_01_generate_schema_db_sigtap_sql.py
import os
import csv
import datetime
# import pandas as pd # Se for usar descrições do Excel, descomente e configure

# --- Configurações ---
CAMINHO_PASTA_LAYOUTS = os.environ.get('SIGTAP_CAMINHO_PASTA_LAYOUTS', r'G:\Meu Drive\Indra company\SECRETARIA_DE_SAUDE_RECIFE\CAPS\GerarSQL_V2\downloads\unzipped\TabelaUnificada_202505_v2505061938')
#CAMINHO_PASTA_LAYOUTS = os.environ.get('SIGTAP_CAMINHO_PASTA_LAYOUTS', r'G:\Meu Drive\Indra company\SECRETARIA_DE_SAUDE_RECIFE\CAPS\GerarSQL_V2\downloads\unzipped\TabelaUnificada_202506_v2506061904')
CAMINHO_SAIDA_SQL = os.environ.get('SIGTAP_CAMINHO_SAIDA_SQL_DDL', r'G:\Meu Drive\Indra company\SECRETARIA_DE_SAUDE_RECIFE\CAPS\GerarSQL_V2\create_db')
NOME_ARQUIVO_SAIDA_SCHEMA = 'create_sigtap_tables_schema_v04-07_07.sql' 

SUFIXO_LAYOUT = '_layout.txt'
TABELA_VERSION = 'tb_version'
COLUNA_VERSION_FK = 'version_id' 
CODIFICACAO_LAYOUT = 'latin-1' 

# --- Configurações para Descrições do Excel (opcional) ---
# CAMINHO_ARQUIVO_DESCRICOES_EXCEL = os.environ.get('SIGTAP_EXCEL_DESCRICOES', r'G:\Meu Drive\Indra company\SECRETARIA DE SAUDE - RECIFE\CAPS\TabelaUnificada_202504_v2504031832\DATASUS - Tabela de Procedimentos - Lay-out.xls')
# NOME_PLANILHA_DESCRICOES = os.environ.get('SIGTAP_EXCEL_PLANILHA', 'Layout dos Arquivos')
# COLUNA_EXCEL_NOME_TABELA = os.environ.get('SIGTAP_EXCEL_COL_TABELA', 'NM_TABELA')
# COLUNA_EXCEL_NOME_CAMPO_LAYOUT = os.environ.get('SIGTAP_EXCEL_COL_CAMPO', 'NM_CAMPO')
# COLUNA_EXCEL_DESCRICAO = os.environ.get('SIGTAP_EXCEL_COL_DESCRICAO', 'DS_CAMPO')
ALL_EXCEL_DESCRIPTIONS = {} # Deixe vazio se não for carregar descrições do Excel

# ----------------------------------------------------------------------------------
# DICIONÁRIO DE CONTROLE DE UNICIDADE COMPOSTA (coluna_co, version_id)
# ----------------------------------------------------------------------------------
# REVISE ESTA LISTA CUIDADOSAMENTE!
# MANTENHA APENAS AS TABELAS ONDE (co_X, version_id) REALMENTE DEVE SER ÚNICO.
# REMOVA as entradas para tabelas onde essa combinação PODE SE REPETIR.
# ----------------------------------------------------------------------------------
COLUNAS_CO_PARA_UNIQUE_COMPOSTA_COM_VERSION = {
    'tb_procedimento': 'co_procedimento', 
    'tb_grupo': 'co_grupo',            
    'tb_cid': 'co_cid',                 
    'tb_ocupacao': 'co_ocupacao',       
    # 'tb_servico': 'co_servico', # Exemplo: Se co_servico pode repetir para mesmo version_id, comente/remova
    'tb_financiamento': 'co_financiamento', # Mantenha se esta DEVE ser única
    'tb_rubrica': 'co_rubrica',
    'tb_detalhe': 'co_detalhe',
    'tb_habilitacao': 'co_habilitacao', # Se esta DEVE ser única
    'tb_rede_atencao': 'co_rede_atencao',
    # 'tb_componente_rede': 'co_componente_rede', # Se esta pode repetir, comente/remova
    'tb_registro': 'co_registro',
    'tb_tipo_leito': 'co_tipo_leito',
    'tb_regra_condicionada': 'co_regra_condicionada', # Se esta DEVE ser única
    'tb_tuss': 'co_tuss',
    'tb_renases': 'co_renases', 
    'tb_modalidade': 'co_modalidade', 
    # 'tb_grupo_habilitacao': 'co_grupo', # Verifique a necessidade de UNIQUE aqui
    # 'tb_descricao_detalhe': 'co_detalhe', # Verifique
    # 'tb_forma_organizacao': 'co_forma_organizacao', # REMOVIDO CONFORME SOLICITADO
    # 'tb_sia_sih': 'co_procedimento_sia_sih',       # REMOVIDO CONFORME SOLICITADO
}

# Chaves Únicas Compostas Especiais (mais de uma coluna 'co_' + version_id)
# REVISE ESTA LISTA TAMBÉM!
UNIQUE_COMPOSTAS_ESPECIAIS_COM_VERSION = {
    # Exemplo: Se tb_servico_classificacao PODE ter (co_servico, co_classificacao, version_id) duplicados,
    # então remova a entrada abaixo. Ela já terá id BIGSERIAL e PK (id, version_id) que garante unicidade da linha.
    # 'tb_servico_classificacao': ['co_servico', 'co_classificacao'] 
}

sql_tb_version = f"""
-- Script SQL gerado para o banco SIGTAP (Schema v2.8)
-- 'id' como BIGSERIAL e PK (id, version_id) para tabelas de dados.
-- UNIQUE constraints (co_X, version_id) aplicadas seletivamente.
-- Data de geração: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

CREATE TABLE IF NOT EXISTS public."{TABELA_VERSION}" (
    id BIGSERIAL PRIMARY KEY,
    competencia VARCHAR(6) UNIQUE NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE public."{TABELA_VERSION}" IS 'Tabela para versionamento das competências dos dados SIGTAP.';
COMMENT ON COLUMN public."{TABELA_VERSION}".id IS 'ID sequencial único da versão (competência).';
COMMENT ON COLUMN public."{TABELA_VERSION}".competencia IS 'Competência no formato AAAAMM.';
COMMENT ON COLUMN public."{TABELA_VERSION}".created_at IS 'Timestamp da criação do registro da versão.';
"""

def parse_layout_metadata(layout_filepath): # Mantida da sua v2.4 (sem descrições do Excel por padrão)
    column_definitions = []
    table_name = None
    try:
        filename = os.path.basename(layout_filepath)
        table_name = filename.replace(SUFIXO_LAYOUT, '').lower()
        if not table_name: return None, None
        with open(layout_filepath, 'r', encoding=CODIFICACAO_LAYOUT) as f:
            reader = csv.reader(f); header_line = next(reader, None)
            if not header_line: return table_name, []
            header = [h.strip().lower() for h in header_line]
            header_map = {h: i for i, h in enumerate(header)}
            required_cols = ['coluna', 'tamanho', 'tipo'] # 'inicio', 'fim' não essenciais para DDL
            missing_cols = [rc for rc in required_cols if rc not in header_map or header_map.get(rc) is None]
            if missing_cols:
                print(f"Erro: Layout {filename} sem colunas obrigatórias: {', '.join(missing_cols)}. Cabeçalho: {header}")
                return table_name, None 
            for i, row in enumerate(reader):
                if not row or not any(field.strip() for field in row) or len(row) < len(header_map): continue 
                try:
                    col_name_raw = row[header_map['coluna']].strip()
                    col_name = col_name_raw.lower().replace(' ', '_').replace('-', '_')
                    tam_str = row[header_map['tamanho']].strip()
                    col_type_raw = row[header_map['tipo']].strip().upper()
                    if not col_name: continue
                    tam = int(tam_str) if tam_str.isdigit() else 0
                    # Mapeamento de tipos (consistente com versões anteriores)
                    if col_type_raw == 'VARCHAR2': col_type = f'VARCHAR({tam})' if tam > 0 else 'TEXT'
                    elif col_type_raw == 'CHAR': col_type = f'CHAR({tam})' if tam > 0 else 'CHAR(1)'
                    elif col_type_raw.startswith('NUMBER'): col_type = 'NUMERIC' 
                    elif col_type_raw == 'DATE' : col_type = 'DATE'
                    elif col_type_raw == 'ALFA' : col_type = f'VARCHAR({tam})' if tam > 0 else 'TEXT'
                    elif col_type_raw == 'NUM' : 
                        if tam > 18: col_type = 'NUMERIC'
                        elif tam > 9: col_type = 'BIGINT' 
                        elif tam > 4: col_type = 'INTEGER' 
                        elif tam > 0: col_type = 'SMALLINT' 
                        else: col_type = 'NUMERIC' 
                    else: col_type = f'VARCHAR({tam})' if tam > 0 else 'TEXT' 
                    
                    description = ALL_EXCEL_DESCRIPTIONS.get(table_name, {}).get(col_name_raw) # Se for usar Excel
                    column_definitions.append({'name': col_name, 'type': col_type, 'raw_name': col_name_raw, 'description': description})
                except (IndexError, ValueError) as e: print(f"Aviso: Pulando linha (índice/valor) layout {filename}, linha {i+2}: {row} - Erro: {e}")
                except KeyError as e: print(f"Aviso: Pulando linha (KeyError: {e}) layout {filename}, linha {i+2}: {row}")
        return table_name, column_definitions
    except UnicodeDecodeError as ude: print(f"Erro encoding layout {layout_filepath} com {CODIFICACAO_LAYOUT}: {ude}"); return None, None
    except FileNotFoundError: print(f"Erro: Layout não encontrado: {layout_filepath}"); return None, None
    except Exception as e: print(f"Erro ao ler/parsear layout {layout_filepath}: {e}"); return None, None

def main():
    print(f"--- Gerador de Scripts SQL CREATE TABLE (Schema v2.8) ---")
    # Se for usar descrições do Excel, certifique-se que a função load_column_descriptions está definida e chamada.
    # global ALL_EXCEL_DESCRIPTIONS; ALL_EXCEL_DESCRIPTIONS = load_column_descriptions(...)
    
    all_sql_scripts = [sql_tb_version]
    table_metadata_for_fks = {} 

    try:
        if not os.path.isdir(CAMINHO_PASTA_LAYOUTS):
            print(f"Erro: Pasta de layouts não encontrada: {CAMINHO_PASTA_LAYOUTS}"); return
        print(f"Lendo layouts de: {CAMINHO_PASTA_LAYOUTS}")
        layout_files = sorted([f for f in os.listdir(CAMINHO_PASTA_LAYOUTS) if f.lower().endswith(SUFIXO_LAYOUT.lower())])
        if not layout_files: print("Nenhum arquivo de layout (*_layout.txt) encontrado."); return

        parsed_layouts = {} 
        for filename in layout_files:
            filepath = os.path.join(CAMINHO_PASTA_LAYOUTS, filename)
            table_name, raw_col_defs = parse_layout_metadata(filepath)
            if table_name and raw_col_defs is not None: 
                if raw_col_defs: parsed_layouts[table_name] = raw_col_defs
            elif table_name and raw_col_defs is None:
                print(f"Aviso: Definições para {table_name} não parseadas. Tabela pulada.")
        
        ordem_para_criar_final = list(parsed_layouts.keys()) # Ordem simples por nome de arquivo, ajuste se necessário
        # Se você tem uma ORDEM_PROCESSAMENTO_TABELAS_BASE para DDL, use-a para ordenar 'ordem_para_criar_final'

        for table_name in ordem_para_criar_final:
            if table_name not in parsed_layouts : continue
            raw_col_defs = parsed_layouts[table_name]
            
            # TODAS as tabelas de dados (tb_ e rl_) terão id BIGSERIAL e version_id
            column_sql_lines = [
                f"    id BIGSERIAL NOT NULL",
                f"    \"{COLUNA_VERSION_FK}\" BIGINT NOT NULL"
            ]
            # Chave primária padrão para todas as tabelas de dados
            pk_definition = f"    PRIMARY KEY (id, \"{COLUNA_VERSION_FK}\")"
            
            # Coleta nomes de colunas para verificações e FKs
            # Inicia com as colunas da PK, pois elas sempre existirão.
            current_table_all_col_names = ['id', COLUNA_VERSION_FK] 

            for col_def in raw_col_defs:
                col_name = col_def['name']
                col_type = col_def['type']
                # 'id' e 'version_id' já foram adicionados.
                if col_name.lower() in ['id', COLUNA_VERSION_FK.lower()]: continue 
                
                line = f"    \"{col_name}\" {col_type}"
                # As colunas do layout agora são apenas colunas de dados.
                # Se alguma delas for uma FK para outra tabela, a constraint de FK será adicionada depois.
                column_sql_lines.append(line)
                current_table_all_col_names.append(col_name)
            
            column_sql_lines.append(pk_definition)
            
            # Adicionar UNIQUE constraints SELETIVAMENTE
            unique_constraints_sql_list = []
            if table_name in COLUNAS_CO_PARA_UNIQUE_COMPOSTA_COM_VERSION:
                col_co_principal = COLUNAS_CO_PARA_UNIQUE_COMPOSTA_COM_VERSION[table_name]
                # Verifica se a coluna co_X realmente existe na tabela (após o parse do layout)
                if col_co_principal and col_co_principal in current_table_all_col_names:
                    constraint_name = f"uq_{table_name}_{col_co_principal.replace('co_','')}_v"
                    unique_constraints_sql_list.append(f"    CONSTRAINT \"{constraint_name}\" UNIQUE (\"{col_co_principal}\", \"{COLUNA_VERSION_FK}\")")
            
            if table_name in UNIQUE_COMPOSTAS_ESPECIAIS_COM_VERSION:
                cols_para_unique_especial = UNIQUE_COMPOSTAS_ESPECIAIS_COM_VERSION[table_name]
                if all(c in current_table_all_col_names for c in cols_para_unique_especial):
                    constraint_name_suffix = "_".join(c.replace('co_','') for c in cols_para_unique_especial)
                    constraint_name = f"uq_{table_name}_{constraint_name_suffix}_v"
                    cols_quoted = [f'"{c}"' for c in cols_para_unique_especial] + [f'"{COLUNA_VERSION_FK}"']
                    unique_constraints_sql_list.append(f"    CONSTRAINT \"{constraint_name}\" UNIQUE ({', '.join(cols_quoted)})")
            
            if unique_constraints_sql_list:
                column_sql_lines.extend(unique_constraints_sql_list)

            # FK para tb_version (todas as tabelas de dados terão esta FK)
            column_sql_lines.append(f"    CONSTRAINT fk_{table_name}_{COLUNA_VERSION_FK} FOREIGN KEY (\"{COLUNA_VERSION_FK}\") REFERENCES public.\"{TABELA_VERSION}\" (id)")

            column_definitions_string = ",\n".join(column_sql_lines)
            sql_script = f"""\n-- Criação da tabela "public.{table_name}"
CREATE TABLE IF NOT EXISTS public."{table_name}" (\n{column_definitions_string}\n);"""
            all_sql_scripts.append(sql_script)
            
            # Adicionar comentários (lógica mantida)
            comment_sql_lines = []
            id_desc = ALL_EXCEL_DESCRIPTIONS.get(table_name, {}).get('id', 'ID sequencial único da linha, gerado automaticamente.')
            comment_sql_lines.append(f'COMMENT ON COLUMN public."{table_name}"."id" IS \'{id_desc.replace("'", "''")}\';')
            version_id_desc = ALL_EXCEL_DESCRIPTIONS.get(table_name, {}).get(COLUNA_VERSION_FK, f'FK para public."{TABELA_VERSION}".')
            comment_sql_lines.append(f'COMMENT ON COLUMN public."{table_name}"."{COLUNA_VERSION_FK}" IS \'{version_id_desc.replace("'", "''")}\';')
            for col_def in raw_col_defs:
                if col_def['name'].lower() in ['id', COLUNA_VERSION_FK.lower()]: continue
                if col_def.get('description'):
                    comment_sql_lines.append(f'COMMENT ON COLUMN public."{table_name}"."{col_def["name"]}" IS \'{col_def["description"].replace("'", "''")}\';')
            if comment_sql_lines:
                all_sql_scripts.append(f"\n-- Comentários para public.\"{table_name}\"")
                all_sql_scripts.extend(comment_sql_lines)

            # Armazena metadados para a criação de FKs entre tabelas de dados
            table_metadata_for_fks[table_name] = {
                'cols': current_table_all_col_names,
                'pk': ['id', COLUNA_VERSION_FK], # PK padrão
                'unique_constraints_tuples': [] # Para as UQs que foram realmente criadas
            }
            if table_name in COLUNAS_CO_PARA_UNIQUE_COMPOSTA_COM_VERSION and COLUNAS_CO_PARA_UNIQUE_COMPOSTA_COM_VERSION[table_name] in current_table_all_col_names:
                table_metadata_for_fks[table_name]['unique_constraints_tuples'].append(
                    (COLUNAS_CO_PARA_UNIQUE_COMPOSTA_COM_VERSION[table_name], COLUNA_VERSION_FK)
                )
            if table_name in UNIQUE_COMPOSTAS_ESPECIAIS_COM_VERSION and all(c in current_table_all_col_names for c in UNIQUE_COMPOSTAS_ESPECIAIS_COM_VERSION[table_name]):
                 table_metadata_for_fks[table_name]['unique_constraints_tuples'].append(
                    tuple(list(UNIQUE_COMPOSTAS_ESPECIAIS_COM_VERSION[table_name]) + [COLUNA_VERSION_FK])
                )

            print(f"Gerado CREATE TABLE para: public.\"{table_name}\" (id BIGSERIAL, PK(id, version_id))")

        # Geração de FKs entre tabelas de dados
        # Esta lógica precisa ser CUIDADOSAMENTE REVISADA.
        # Se uma FK deve apontar para uma (co_X, version_id) que NÃO É MAIS UNIQUE,
        # ela não pode ser criada dessa forma. Teria que apontar para (id, version_id) da tabela alvo,
        # o que significa que a tabela fonte precisaria ter uma coluna para o 'id' da tabela alvo.
        print("\nGerando restrições FOREIGN KEY (ALTER TABLE) entre tabelas de dados...")
        fk_scripts_alter = []
        for source_table_name, meta_source in table_metadata_for_fks.items():
            if source_table_name == TABELA_VERSION: continue

            source_layout_cols = parsed_layouts.get(source_table_name, [])
            for col_def_source in source_layout_cols: # Itera sobre as colunas do layout original
                col_name_source_fk_candidate = col_def_source['name'] # Nome normalizado da coluna fonte

                # Heurística: colunas 'co_...' que não são 'id' ou 'version_id' podem ser FKs
                if col_name_source_fk_candidate.startswith('co_') and \
                   col_name_source_fk_candidate not in ['id', COLUNA_VERSION_FK]:
                    
                    potential_target_base = col_name_source_fk_candidate.replace('co_', '', 1)
                    possible_target_table_names = [f"tb_{potential_target_base}", f"rl_{potential_target_base}"]
                    
                    for target_table_name in possible_target_table_names:
                        if target_table_name in table_metadata_for_fks and target_table_name != source_table_name:
                            target_meta = table_metadata_for_fks[target_table_name]
                            
                            # Tenta criar FK para uma UNIQUE constraint (co_X, version_id) existente na tabela alvo
                            # A coluna co_X na tabela alvo deve ser igual a col_name_source_fk_candidate
                            target_unique_key_candidate = (col_name_source_fk_candidate, COLUNA_VERSION_FK)

                            if target_unique_key_candidate in target_meta.get('unique_constraints_tuples', []):
                                fk_source_columns = (col_name_source_fk_candidate, COLUNA_VERSION_FK)
                                fk_target_columns = target_unique_key_candidate

                                fk_src_quoted = [f'"{c}"' for c in fk_source_columns]
                                fk_tgt_quoted = [f'"{c}"' for c in fk_target_columns]
                                constraint_name = f"fk_{source_table_name}_{col_name_source_fk_candidate}_to_{target_table_name}"
                                
                                fk_sql = f"""\nALTER TABLE public."{source_table_name}" ADD CONSTRAINT "{constraint_name}"
FOREIGN KEY ({', '.join(fk_src_quoted)}) REFERENCES public."{target_table_name}" ({', '.join(fk_tgt_quoted)});"""
                                if fk_sql not in fk_scripts_alter:
                                    fk_scripts_alter.append(fk_sql)
                                    print(f"  FK: {source_table_name}({', '.join(fk_source_columns)}) -> {target_table_name}({', '.join(fk_target_columns)})")
                                break # Achou um alvo e criou a FK
                            # else:
                                # Se não houver uma UNIQUE (co_X, version_id) na tabela alvo,
                                # uma FK para (id, version_id) da tabela alvo seria mais complexa,
                                # pois a tabela fonte precisaria ter uma coluna 'co_alvo_id' e 'co_alvo_version_id'.
                                # print(f"  Aviso: Não foi possível criar FK automática para {source_table_name}.{col_name_source_fk_candidate} -> {target_table_name} via co_coluna. Verifique manualmente.")
                    break # Processou esta coluna candidata a FK
        all_sql_scripts.extend(fk_scripts_alter)

    except Exception as e:
        print(f"Erro inesperado: {e}"); import traceback; traceback.print_exc()

    full_sql_output = "\n\n".join(all_sql_scripts)
    if not os.path.isdir(CAMINHO_SAIDA_SQL):
        try: os.makedirs(CAMINHO_SAIDA_SQL, exist_ok=True); print(f"Pasta de saída criada: {CAMINHO_SAIDA_SQL}")
        except OSError as e: print(f"Erro ao criar pasta de saída {CAMINHO_SAIDA_SQL}: {e}"); return
    final_output_path = os.path.join(CAMINHO_SAIDA_SQL, NOME_ARQUIVO_SAIDA_SCHEMA)
    try:
        with open(final_output_path, "w", encoding='utf-8') as f: f.write(full_sql_output)
        print(f"\nScript de schema SQL gerado em {final_output_path}")
    except IOError as e: print(f"Erro ao salvar script SQL em {final_output_path}: {e}")

if __name__ == '__main__':
    main()