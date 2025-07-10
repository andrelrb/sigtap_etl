# run_app.py
from app import app

if __name__ == "__main__":
    # O debug=True permite que as alterações no código recarreguem o servidor automaticamente
    # host='0.0.0.0' permite acessar de outros dispositivos na mesma rede
    app.run(host='0.0.0.0', port=5000, debug=True)