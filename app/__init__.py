# app/__init__.py
# Versão: 2.1 (Correção de erro de decodificação Unicode)
# Descrição: Aplicação Flask que orquestra a execução dos scripts, agora com
#            tratamento de erros de codificação de texto para rodar no Windows.

from flask import Flask, render_template, jsonify, request
import configparser
import subprocess
import os
import sys

# Cria a aplicação Flask
app = Flask(__name__)

# --- Funções Auxiliares ---
def get_project_root():
    """Encontra e retorna o caminho absoluto da pasta raiz do projeto."""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def load_config():
    """Carrega as configurações do config.ini."""
    root_path = get_project_root()
    config_path = os.path.join(root_path, 'config.ini')
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Arquivo 'config.ini' não encontrado em {root_path}")
    
    config = configparser.ConfigParser()
    config.read(config_path)
    return config

# --- Rotas da Aplicação ---

@app.route('/')
def index():
    """Renderiza a página principal (index.html)."""
    try:
        config = load_config()
        db_config = dict(config['DATABASE'])
        return render_template('index.html', db_config=db_config)
    except Exception as e:
        error_message = f"<h1>Erro ao carregar a configuração</h1><p>Verifique se o arquivo 'config.ini' existe na raiz do projeto e contém a seção [DATABASE].</p><p><b>Detalhe do Erro:</b> {e}</p>"
        return error_message, 500

@app.route('/run_step', methods=['POST'])
def run_step():
    """
    Rota genérica que executa um script Python quando chamada pela interface.
    """
    step_id = request.json.get('step')
    root_path = get_project_root()
    
    scripts_map = {
        # Mapeia o ID do botão para o nome do arquivo na pasta /src
        "extraction": "step_00_extraction.py",
        "generate_schema": "Step_01_generate_schema_db_sigtap_sql.py",
        "execute_schema": "step_02_create_table_sigtap.py",
        "generate_inserts": "step_03_unified_generate_inserts.py",
        "execute_inserts": "step_04_executor_scripts_sql.py"
    }

    script_name = scripts_map.get(step_id)
    if not script_name:
        return jsonify({"status": "error", "output": f"Erro: Passo '{step_id}' não reconhecido pelo orquestrador."})

    script_path = os.path.join(root_path, 'src', script_name)
    
    if not os.path.exists(script_path):
        return jsonify({"status": "error", "output": f"Erro: Script não encontrado no caminho esperado: {script_path}"})

    try:
        print(f"Executando script para a etapa '{step_id}': {script_name}")

        # --- ALTERAÇÃO PRINCIPAL APLICADA AQUI ---
        # Adicionado errors='replace' para evitar o UnicodeDecodeError.
        # Isso substitui caracteres problemáticos em vez de quebrar a aplicação.
        process = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace', # <<-- CORREÇÃO APLICADA
            check=False
        )
        # --- FIM DA ALTERAÇÃO ---
        
        output = process.stdout + process.stderr
        
        if process.returncode == 0:
            status = "success"
            print(f"Etapa '{step_id}' concluída com sucesso.")
        else:
            status = "error"
            print(f"Etapa '{step_id}' concluída com erro. Código de retorno: {process.returncode}")
        
    except Exception as e:
        output = f"Ocorreu um erro inesperado ao tentar executar o script:\n{str(e)}"
        status = "error"
        
    return jsonify({"status": status, "output": output})