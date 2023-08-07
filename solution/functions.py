import pandas as pd
from openpyxl import load_workbook
from datetime import datetime
import shutil

def load_worksheet(workbook, worksheet):
    """Carrega uma planilha específica de um arquivo de trabalho (workbook) e retorna a planilha desejada."""
    workbook = load_workbook(workbook)
    return workbook[worksheet]

def data_extract(cache_data):
    rows = []
    dims = {}

    for cf in cache_data.cacheFields:
        dims[cf.name] = cf.name
        print(f"Nome da coluna extraída: {cf.name}") # Verificando se os nomes das colunas estão sendo extraídos corretamente

    for dat in cache_data.records.r:
        data = []
        for cols in dat:
            try:
                data.append(cols.v)
            except Exception as e:
                print(f"Error extracting value: {e}")
                data.append(None)
        rows.append(data)
        
    df = pd.DataFrame(columns=dims, data=rows)
    return df


def transform_data(table):
    """
    Transforma um DataFrame da ANP para o formato desejado.
    - Agrega os meses em uma única coluna 'mes'.
    - Ajusta os nomes das colunas.
    - Lida com valores nulos, preenchendo com 0.
    - Cria a coluna 'year_month' a partir de 'ANO' e 'mes'.
    - Converte 'year_month' para o formato de data.
    - Remove a coluna 'mes'.
    - Adiciona uma coluna 'created_at' com a data de hoje.
    """
    transformed_df = pd.melt(
        table,
        id_vars=['COMBUSTÍVEL', 'REGIÃO', 'ANO', 'ESTADO', 'UNIDADE', 'TOTAL'],
        var_name='mes',
        value_name='volume'
    )
    
    # Mapeia os nomes dos meses para números
    month_mapping = {
        'Jan': '01', 'Fev': '02', 'Mar': '03', 'Abr': '04',
        'Mai': '05', 'Jun': '06', 'Jul': '07', 'Ago': '08',
        'Set': '09', 'Out': '10', 'Nov': '11', 'Dez': '12'
    }
    transformed_df['mes'] = transformed_df['mes'].replace(month_mapping)
    
    transformed_df.rename(
        columns={
            "ESTADO": "uf",
            "ANO": "year_month",
            "UNIDADE": "unit",
            "COMBUSTÍVEL": "product"
        },
        inplace=True
    )
    
    transformed_df.fillna(0, inplace=True)  # Preenche valores nulos com 0

    transformed_df['year_month'] = transformed_df['year_month'] + transformed_df['mes']
    transformed_df['year_month'] = pd.to_datetime(transformed_df['year_month'], format="%Y%m")
    
    transformed_df.drop(['mes'], axis=1, inplace=True)
    transformed_df['created_at'] = pd.to_datetime('today')

    return transformed_df
