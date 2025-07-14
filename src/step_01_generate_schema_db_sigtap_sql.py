# Nome do arquivo: Step_01_generate_schema_db_sigtap_sql.py
# Versao do Arquivo: v3.2 (Remove UNIQUE da coluna competencia em tb_version)
# Descrição: Gera o script DDL (CREATE TABLE) para o schema do banco de dados SIGTAP.
#            Esta versão remove a restrição UNIQUE da coluna 'competencia' para
#            permitir o recarregamento de dados da mesma competência.

import os
import csv
import datetime
import re
import traceback

def setup_automatico_paths_e_nomes():
    """
    Função central para configurar os caminhos e nomes de arquivo dinamicamente.
    Retorna um dicionário com as configurações prontas para uso.
    """
    configs = {}
    project_root = os.path.abspath(os.path.dirname(__file__))
    while not any(os.path.exists(os.path.join(project_root, f)) for f in ['.git', 'README.md', 'requirements.txt']):
        parent = os.path.dirname(project_root)
        if parent == project_root:
            print("AVISO: Não foi possível detectar a raiz do projeto. Usando diretório atual.")
            project_root = os.path.abspath(os.path.dirname(__file__))
            break
        project_root = parent

    unzipped_base_dir = os.path.join(project_root, 'downloads', 'unzipped')
    if not os.path.isdir(unzipped_base_dir):
        raise FileNotFoundError(f"Erro Crítico: A pasta base de layouts não foi encontrada em '{unzipped_base_dir}'.")

    all_layout_dirs = [d for d in os.listdir(unzipped_base_dir) if os.path.isdir(os.path.join(unzipped_base_dir, d))]
    if not all_layout_dirs:
        raise FileNotFoundError(f"Erro Crítico: Nenhuma pasta de layout encontrada em '{unzipped_base_dir}'.")

    latest_layout_dir_name = max(all_layout_dirs, key=lambda d: os.path.getmtime(os.path.join(unzipped_base_dir, d)))
    configs['CAMINHO_PASTA_LAYOUTS'] = os.path.join(unzipped_base_dir, latest_layout_dir_name)

    configs['CAMINHO_SAIDA_SQL'] = os.path.join(project_root, 'create_db')
    os.makedirs(configs['CAMINHO_SAIDA_SQL'], exist_ok=True)

    competencia_match = re.search(r'(\d{6})', latest_layout_dir_name)
    if not competencia_match:
        raise ValueError("Não foi possível extrair a competência (AAAAMM) do nome da pasta de layout.")
    competencia = competencia_match.group(1)

    version = 1
    while True:
        nome_arquivo = f"create_sigtap_tables_schema_{competencia}_v{version:02d}.sql"
        caminho_completo = os.path.join(configs['CAMINHO_SAIDA_SQL'], nome_arquivo)
        if not os.path.exists(caminho_completo):
            configs['NOME_ARQUIVO_SAIDA_SCHEMA'] = nome_arquivo
            break
        version += 1
    return configs

# --- Configurações Estáticas ---
SUFIXO_LAYOUT = '_layout.txt'
TABELA_VERSION = 'tb_version'
COLUNA_VERSION_FK = 'version_id'
CODIFICACAO_LAYOUT = 'latin-1'

# Dicionários de controle de unicidade (mantidos da sua versão)
COLUNAS_CO_PARA_UNIQUE_COMPOSTA_COM_VERSION = {
    'tb_procedimento': 'co_procedimento', 'tb_grupo': 'co_grupo', 'tb_cid': 'co_cid', 'tb_ocupacao': 'co_ocupacao',
    'tb_financiamento': 'co_financiamento', 'tb_rubrica': 'co_rubrica', 'tb_detalhe': 'co_detalhe',
    'tb_habilitacao': 'co_habilitacao', 'tb_rede_atencao': 'co_rede_atencao', 'tb_registro': 'co_registro',
    'tb_tipo_leito': 'co_tipo_leito', 'tb_regra_condicionada': 'co_regra_condicionada', 'tb_tuss': 'co_tuss',
    'tb_renases': 'co_renases', 'tb_modalidade': 'co_modalidade',
}
UNIQUE_COMPOSTAS_ESPECIAIS_COM_VERSION = {}

def parse_layout_metadata(layout_filepath):
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
            required_cols = ['coluna', 'tamanho', 'tipo']
            if any(rc not in header_map for rc in required_cols): return table_name, None
            for i, row in enumerate(reader):
                if not row or not any(field.strip() for field in row): continue
                col_name_raw = row[header_map['coluna']].strip()
                col_name = col_name_raw.lower().replace(' ', '_').replace('-', '_')
                tam_str = row[header_map['tamanho']].strip()
                col_type_raw = row[header_map['tipo']].strip().upper()
                if not col_name: continue
                tam = int(tam_str) if tam_str.isdigit() else 0
                if col_type_raw == 'VARCHAR2': col_type = f'VARCHAR({tam})' if tam > 0 else 'TEXT'
                elif col_type_raw == 'CHAR': col_type = f'CHAR({tam})' if tam > 0 else 'CHAR(1)'
                elif col_type_raw.startswith('NUMBER'): col_type = 'NUMERIC'
                elif col_type_raw == 'DATE' : col_type = 'DATE'
                elif col_type_raw == 'ALFA' : col_type = f'VARCHAR({tam})' if tam > 0 else 'TEXT'
                elif col_type_raw == 'NUM' :
                    if tam > 18: col_type = 'NUMERIC'
                    elif tam > 9: col_type = 'BIGINT'
                    elif tam > 4: col_type = 'INTEGER'
                    else: col_type = 'SMALLINT'
                else: col_type = 'TEXT'
                column_definitions.append({'name': col_name, 'type': col_type, 'raw_name': col_name_raw, 'description': None})
        return table_name, column_definitions
    except Exception as e:
        print(f"Erro ao ler/parsear layout {layout_filepath}: {e}")
        return None, None

def main():
    try:
        configs = setup_automatico_paths_e_nomes()
    except (FileNotFoundError, ValueError) as e:
        print(f"\n{e}\nProcesso abortado.")
        return

    print(f"\n--- Gerador de Scripts SQL CREATE TABLE (Schema v3.2) ---")
    print(f"Lendo layouts de: {configs['CAMINHO_PASTA_LAYOUTS']}")
    
    # --- ALTERAÇÃO PRINCIPAL APLICADA AQUI ---
    # Removido o 'UNIQUE' da coluna 'competencia' para permitir recargas.
    sql_tb_version = f"""
-- Script SQL gerado para o banco SIGTAP
-- Arquivo: {configs['NOME_ARQUIVO_SAIDA_SCHEMA']}
-- Data de geração: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

CREATE TABLE IF NOT EXISTS public."{TABELA_VERSION}" (
    id BIGSERIAL PRIMARY KEY,
    competencia VARCHAR(6) NOT NULL, -- UNIQUE foi REMOVIDO daqui
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE public."{TABELA_VERSION}" IS 'Tabela para versionamento. Permite múltiplas entradas para a mesma competência.';
"""
    # --- FIM DA ALTERAÇÃO PRINCIPAL ---
    
    all_sql_scripts = [sql_tb_version]
    
    try:
        layout_files = sorted([f for f in os.listdir(configs['CAMINHO_PASTA_LAYOUTS']) if f.lower().endswith(SUFIXO_LAYOUT.lower())])
        if not layout_files:
            print("Nenhum arquivo de layout (*_layout.txt) encontrado.")
            return

        parsed_layouts = {}
        for filename in layout_files:
            filepath = os.path.join(configs['CAMINHO_PASTA_LAYOUTS'], filename)
            table_name, raw_col_defs = parse_layout_metadata(filepath)
            if table_name and raw_col_defs:
                parsed_layouts[table_name] = raw_col_defs

        for table_name in parsed_layouts:
            raw_col_defs = parsed_layouts[table_name]
            column_sql_lines = [
                f"    id BIGSERIAL NOT NULL",
                f"    \"{COLUNA_VERSION_FK}\" BIGINT NOT NULL"
            ]
            pk_definition = f"    PRIMARY KEY (id, \"{COLUNA_VERSION_FK}\")"
            current_table_all_col_names = ['id', COLUNA_VERSION_FK]

            for col_def in raw_col_defs:
                col_name = col_def['name']
                if col_name.lower() in ['id', COLUNA_VERSION_FK.lower()]: continue
                column_sql_lines.append(f"    \"{col_name}\" {col_def['type']}")
                current_table_all_col_names.append(col_name)
            
            column_sql_lines.append(pk_definition)
            
            unique_constraints_sql_list = []
            if table_name in COLUNAS_CO_PARA_UNIQUE_COMPOSTA_COM_VERSION:
                col_co = COLUNAS_CO_PARA_UNIQUE_COMPOSTA_COM_VERSION[table_name]
                if col_co in current_table_all_col_names:
                    constraint_name = f"uq_{table_name}_{col_co.replace('co_','')}_v"
                    unique_constraints_sql_list.append(f"    CONSTRAINT \"{constraint_name}\" UNIQUE (\"{col_co}\", \"{COLUNA_VERSION_FK}\")")
            
            if unique_constraints_sql_list:
                column_sql_lines.extend(unique_constraints_sql_list)

            column_sql_lines.append(f"    CONSTRAINT fk_{table_name}_{COLUNA_VERSION_FK} FOREIGN KEY (\"{COLUNA_VERSION_FK}\") REFERENCES public.\"{TABELA_VERSION}\" (id)")
            
            column_definitions_string = ",\n".join(column_sql_lines)
            sql_script = f"""\n-- Criação da tabela "public.{table_name}"
CREATE TABLE IF NOT EXISTS public."{table_name}" (\n{column_definitions_string}\n);"""
            all_sql_scripts.append(sql_script)
            
            print(f"Gerado CREATE TABLE para: public.\"{table_name}\"")
        
        # Salvando o arquivo
        full_sql_output = "\n\n".join(all_sql_scripts)
        final_output_path = os.path.join(configs['CAMINHO_SAIDA_SQL'], configs['NOME_ARQUIVO_SAIDA_SCHEMA'])
        with open(final_output_path, "w", encoding='utf-8') as f:
            f.write(full_sql_output)
        print(f"\nScript de schema SQL gerado com sucesso em: {final_output_path}")

    except Exception as e:
        print(f"Erro inesperado durante a geração do SQL: {e}")
        traceback.print_exc()

if __name__ == '__main__':
    main()