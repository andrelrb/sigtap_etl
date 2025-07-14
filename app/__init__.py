# app/__init__.py
# Versão: 2.3 (Adiciona verificação de conexão com o banco)
# Descrição: Aplicação Flask que orquestra os scripts e agora inclui uma
#            rota para testar a conexão com o banco de dados diretamente da interface.

from flask import Flask, render_template, jsonify, request
import configparser
import subprocess
import os
import sys
import psycopg2 # Importado para o teste de conexão

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

def execute_script(script_name):
    """Função reutilizável para executar um script e retornar o resultado."""
    root_path = get_project_root()
    script_path = os.path.join(root_path, 'src', script_name) # Assumindo que os scripts estão em /src
    
    if not os.path.exists(script_path):
        return {"status": "error", "output": f"Erro: Script não encontrado: {script_path}"}

    try:
        print(f"Executando script: {script_name}")
        process = subprocess.run(
            [sys.executable, script_path],
            capture_output=True, text=True, encoding='utf-8', errors='replace',
            check=True
        )
        return {"status": "success", "output": process.stdout}
    except subprocess.CalledProcessError as e:
        output = e.stdout + "\n" + e.stderr
        print(f"Script {script_name} falhou. Código de retorno: {e.returncode}")
        return {"status": "error", "output": output}
    except Exception as e:
        return {"status": "error", "output": f"Erro inesperado ao executar {script_name}:\n{str(e)}"}

# --- Rotas da Aplicação ---

@app.route('/')
def index():
    """Renderiza a página principal (index.html)."""
    try:
        config = load_config()
        db_config = dict(config['DATABASE'])
        return render_template('index.html', db_config=db_config)
    except Exception as e:
        error_message = f"<h1>Erro ao carregar a configuração</h1><p>Verifique o 'config.ini'.</p><p><b>Detalhe:</b> {e}</p>"
        return error_message, 500

# --- LÓGICA CORRIGIDA AQUI ---
@app.route('/run_step', methods=['POST'])
def run_step():
    """
    Rota que executa um script OU uma ação específica, como testar o banco.
    """
    step_id = request.json.get('step')
    
    # LÓGICA ESPECIAL PARA O BOTÃO DE TESTE DE CONEXÃO
    if step_id == 'check_db_connection':
        try:
            config = load_config()
            db_config = dict(config['DATABASE'])
            print("Testando conexão com o banco de dados...")
            conn = psycopg2.connect(**db_config)
            conn.close()
            print("Conexão bem-sucedida.")
            return jsonify({
                "status": "success",
                "output": "Conexão com o banco de dados foi um SUCESSO!\nAs configurações no config.ini estão corretas."
            })
        except Exception as e:
            print(f"Falha na conexão com o banco: {e}")
            return jsonify({
                "status": "error",
                "output": f"FALHA ao conectar com o banco de dados.\n\n- Verifique se o serviço do PostgreSQL está rodando.\n- Verifique as credenciais no arquivo config.ini.\n\nDetalhe do Erro: {e}"
            })

    # LÓGICA PARA EXECUTAR SCRIPTS (mantida como antes)
    scripts_map = {
        "extraction": "step_00_extraction.py",
        "generate_schema": "Step_01_generate_schema_db_sigtap_sql.py",
        "execute_schema": "step_02_create_table_sigtap.py",
        "generate_inserts": "step_03_unified_generate_inserts.py",
        "execute_inserts": "step_04_executor_scripts_sql.py"
    }

    script_name = scripts_map.get(step_id)
    if not script_name:
        # Se chegar aqui com 'check_db_connection', é porque o IF acima falhou
        return jsonify({"status": "error", "output": f"Erro: Passo '{step_id}' não reconhecido."})

    result = execute_script(script_name)
    return jsonify(result)

@app.route('/run_population_pipeline', methods=['POST'])
def run_population_pipeline():
    """Executa o pipeline de população (passos 4 e 5) em sequência."""
    full_output = []
    
    script_name_step4 = "step_03_unified_generate_inserts.py"
    full_output.append(f"--- INICIANDO ETAPA 4: Gerar Scripts de INSERT ---\n")
    result_step4 = execute_script(script_name_step4)
    full_output.append(result_step4['output'])
    
    if result_step4['status'] == 'error':
        full_output.append("\nERRO: A geração de inserts falhou. A execução no banco foi abortada.")
        return jsonify({"status": "error", "output": "".join(full_output)})

    script_name_step5 = "step_04_executor_scripts_sql.py"
    full_output.append(f"\n\n--- INICIANDO ETAPA 5: Executar Scripts no Banco ---\n")
    result_step5 = execute_script(script_name_step5)
    full_output.append(result_step5['output'])

    final_status = result_step5['status']
    return jsonify({"status": final_status, "output": "".join(full_output)})