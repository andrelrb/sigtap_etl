import os
import pandas as pd

def verificar_dados():
    """
    Verifica os dados nos arquivos da pasta "Insert_09052025".
    """
    
    pasta = r'G:\Meu Drive\Indra company\SECRETARIA_DE_SAUDE_RECIFE\CAPS\GerarSQL\insert_09052025'
    if not os.path.exists(pasta):
        print(f"Pasta '{pasta}' não encontrada.")
        return

    for arquivo in os.listdir(pasta):
        if arquivo.endswith(".csv"):  # Ajuste para a extensão correta dos seus arquivos
            caminho_arquivo = os.path.join(pasta, arquivo)
            try:
                df = pd.read_csv(caminho_arquivo, encoding='latin-1', sep=';')  # Ajuste o separador se necessário
                print(f"Arquivo: {arquivo}")
                print(df.head())  # Exibe as primeiras linhas para inspeção
                print(f"Número de linhas: {len(df)}")
                # Adicione aqui outras verificações ou processamento dos dados
            except Exception as e:
                print(f"Erro ao ler o arquivo {arquivo}: {e}")

if __name__ == "__main__":
    verificar_dados()
