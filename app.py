from flask import Flask, render_template, request, redirect, url_for
import psycopg2
import datetime
import os # Adicionado para usar variáveis de ambiente ou configurar

app = Flask(__name__)

# --- Configurações do Banco de Dados ---
# É ALTAMENTE RECOMENDÁVEL NÃO DEIXAR SENHAS DIRETAMENTE NO CÓDIGO!
# Use variáveis de ambiente, arquivos de configuração seguros, ou um vault de seguranÇa.
DB_HOST = os.environ.get('DB_HOST', 'localhost') # Altere 'localhost' se seu banco estiver em outro servidor
DB_NAME = os.environ.get('DB_NAME', 'sigtap') # Altere 'sigtap_db' para o nome do seu banco
DB_USER = os.environ.get('DB_USER', 'postgres') # <-- COLOQUE SEU USUÁRIO DO BANCO
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'root') # <-- COLOQUE SUA SENHA DO BANCO
DB_PORT = os.environ.get('DB_PORT', '5432') # Porta padrão do PostgreSQL


def get_db_connection():
    """Cria e retorna uma conexão com o banco de dados."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        # Em um ambiente de produção, você pode querer logar isso ou mostrar uma página de erro melhor
        return None


@app.route('/')
def index():
    """Renderiza a página inicial com o formulário."""
    return render_template('index.html')


@app.route('/add_version', methods=['POST'])
def add_version():
    """Processa o formulário e insere a nova versão no banco."""
    competencia = request.form.get('competencia')

    if not competencia:
        # Poderia retornar uma mensagem de erro na página
        print("Erro: Competência não fornecida.")
        return redirect(url_for('index')) # Redireciona de volta para o formulário

    conn = None # Inicializa conexão como None
    try:
        conn = get_db_connection()
        if conn is None:
            # Se a conexão falhar, redireciona ou mostra erro
            print("Não foi possível conectar ao banco de dados.")
            return "Erro interno do servidor", 500 # Retorna um erro HTTP simples

        cur = conn.cursor()

        # SQL para inserir na tb_version.
        # Não fornecemos 'id' pois assumimos que é BIGSERIAL ou tem sequence padrão.
        # Usamos EXTRACT(EPOCH FROM NOW()) para obter o timestamp atual em segundos (BIGINT).
        sql = """
        INSERT INTO tb_version (competencia, created_at)
        VALUES (%s, EXTRACT(EPOCH FROM NOW()));
        """

        cur.execute(sql, (competencia,))

        conn.commit() # Confirma a transação
        cur.close()
        print(f"Versão com competência '{competencia}' inserida com sucesso.")

    except psycopg2.Error as e:
        conn.rollback() # Desfaz a transação em caso de erro
        print(f"Erro do banco de dados ao inserir versão: {e}")
        # Em produção, você pode querer logar isso ou mostrar uma página de erro
        return "Erro do banco de dados", 500
    except Exception as e:
         print(f"Erro inesperado: {e}")
         return "Erro inesperado", 500
    finally:
        if conn:
            conn.close() # Fecha a conexão

    # Redireciona de volta para a página inicial após sucesso
    return redirect(url_for('index'))

# Código para executar a aplicação Flask
if __name__ == '__main__':
    # Em ambiente de desenvolvimento, use debug=True
    # Em produção, desative debug=True e use um servidor WSGI (como Gunicorn ou uWSGI)
    app.run(debug=True)