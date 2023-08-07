import pandas as pd
from functions import *  # Importa as funções definidas no arquivo functions.py
import argparse  # Importa a biblioteca para lidar com argumentos de linha de comando

# Função para analisar os argumentos de linha de comando e obter os diretórios de entrada e saída
def parse_arguments():
    parser = argparse.ArgumentParser(description="ANP Fuel Sales ETL")  # Cria um analisador de argumentos
    parser.add_argument("--input_dir", type=str, default="extracted", help="Directory for input files")  # Argumento para o diretório de entrada
    parser.add_argument("--output_dir", type=str, default="transformed", help="Directory for output files")  # Argumento para o diretório de saída
    args = parser.parse_args()  # Analisa os argumentos passados na linha de comando
    return args  # Retorna os argumentos analisados

# Função para realizar a extração de dados
def etl_oils_fuels():
    args = parse_arguments()  # Obtém os argumentos de linha de comando
    input_dir = args.input_dir  # Obtém o diretório de entrada dos argumentos
    output_dir = args.output_dir  # Obtém o diretório de saída dos argumentos

    # Carrega a planilha e extrai os dados de derivados de petróleo e diesel
    def load_file():
        ws = load_worksheet(workbook="vendas-combustiveis-m3.xlsx", worksheet="Plan1")
        pivot_oil_deriv = ws._pivots[3].cache
        oils = data_extract(pivot_oil_deriv)
        oils.to_csv(f"{input_dir}/oils.csv", index=False)        
        print("Dados de derivados de petróleo extraídos")
        
        pivot_diesel = ws._pivots[1].cache
        diesel = data_extract(pivot_diesel)
        diesel.to_csv(f"{input_dir}/diesel.csv", index=False)
        print('Dados de diesel extraídos')

    load_file()

# Função para transformar e salvar os dados de derivados de petróleo
def transform_oils():
    args = parse_arguments()  # Obtém os argumentos de linha de comando
    input_dir = args.input_dir  # Obtém o diretório de entrada dos argumentos
    output_dir = args.output_dir  # Obtém o diretório de saída dos argumentos

    raw_oil_df = pd.read_csv(f"{input_dir}/oils.csv")  # Lê o arquivo CSV de derivados de petróleo
    transformed_df = transform_data(raw_oil_df)  # Aplica a transformação aos dados
    
    if (transformed_df['volume'].sum() / raw_oil_df['TOTAL'].sum() >= 0.9999):
        # Verifica se os dados transformados têm integridade semelhante aos dados originais
        print('Pronto para processar')
        try:
            # Salva os dados transformados em um arquivo Parquet comprimido no diretório de saída
            transformed_df.to_parquet(f"{output_dir}/oil.gzip", compression='gzip')
            print('Arquivo Parquet comprimido (gzip) criado')
        except Exception as e:
            print('Erro ao criar o arquivo Parquet comprimido:', e)
    else: 
        print('Os dados não estão corretos')

# Função para transformar e salvar os dados de diesel
def transform_diesel():
    args = parse_arguments()  # Obtém os argumentos de linha de comando
    input_dir = args.input_dir  # Obtém o diretório de entrada dos argumentos
    output_dir = args.output_dir  # Obtém o diretório de saída dos argumentos

    raw_diesel_df = pd.read_csv(f"{input_dir}/diesel.csv")  # Lê o arquivo CSV de diesel
    transformed_df = transform_data(raw_diesel_df)  # Aplica a transformação aos dados
    
    if (transformed_df['volume'].sum() / raw_diesel_df['TOTAL'].sum() >= 0.9999):
        # Verifica se os dados transformados têm integridade semelhante aos dados originais
        try:
            transformed_df.drop(['mes'], axis=1, inplace=True)  # Remove a coluna 'mes' dos dados transformados
            # Salva os dados transformados em um arquivo Parquet comprimido no diretório de saída
            transformed_df.to_parquet(f"{output_dir}/diesel.gzip", compression='gzip')
            print('Arquivo Parquet comprimido (gzip) criado')
        except Exception as e:
            print('Erro ao criar o arquivo Parquet comprimido:', e)
    else: 
        print('Os dados não estão corretos')

# Chama as funções para executar o fluxo de ETL
etl_oils_fuels()
transform_oils()
transform_diesel()
