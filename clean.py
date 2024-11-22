import numpy as np
import pandas as pd

import dask.dataframe as dd


# def clean_data(
#     df,
#     datetime_columns=None,
#     numeric_fill=0,
#     categorical_fill="UNKNOWN",
#     drop_na_cols=None,
#     drop_duplicates=True,
# ):
#     """
#     Função genérica para limpar um DataFrame.

#     Parâmetros:
#         df (pd.DataFrame): O DataFrame a ser limpo.
#         datetime_columns (list): Lista de colunas a serem convertidas para datetime.
#         numeric_fill (int/float): Valor para preencher colunas numéricas nulas.
#         categorical_fill (str): Valor para preencher colunas categóricas nulas.
#         drop_na_cols (list): Lista de colunas em que linhas com NaN serão removidas.
#         drop_duplicates (bool): Indica se duplicatas devem ser removidas.

#     Retorna:
#         pd.DataFrame: O DataFrame limpo.
#     """
#     # Converter colunas para datetime
#     if datetime_columns:
#         for col in datetime_columns:
#             if col in df.columns:
#                 df[col] = pd.to_datetime(df[col], errors="coerce")

#     # Preencher valores nulos
#     for col in df.select_dtypes(include=[np.number]).columns:
#         df[col] = df[col].fillna(numeric_fill)

#     for col in df.select_dtypes(include=["object"]).columns:
#         df[col] = df[col].fillna(categorical_fill)

#     # Remover linhas com NaN em colunas específicas
#     if drop_na_cols:
#         df = df.dropna(subset=drop_na_cols)

#     # Remover duplicatas, se necessário
#     if drop_duplicates:
#         df = df.drop_duplicates()

#     return df


def clean_geracao(geracao_path):
    geracao = dd.read_csv(geracao_path)

    geracao = geracao.dropna(subset=["id_usina"])

    geracao["id_usina"] = geracao["id_usina"].astype(int)

    return geracao



def clean_unidade_consumidora(file_path: str):
    unidade_consumidora = pd.read_csv(file_path)

    # remover duplicatas
    unidade_consumidora.drop_duplicates(inplace=True)

    # excluir todos os registros que não possuem id_endereco
    unidade_consumidora = unidade_consumidora.dropna(subset=["id_endereco"])

    # converter todos as colunas id_endereco para inteiros base64
    unidade_consumidora["id_endereco"] = unidade_consumidora["id_endereco"].astype(int)

    return unidade_consumidora

def clean_usina(file_path: str):
    usina = pd.read_csv(file_path)

    # remover duplicatas
    usina.drop_duplicates(inplace=True)

    # excluir todos os registros que não possuem id_endereco
    usina = usina.dropna(subset=["last_plant_history_id"])

    # Por algum motivo o pandas converte para float, apos o processa de remocao e dropna, a coluna last_plant_history_id
    # converter todos as colunas id_endereco para inteiros base64
    usina["last_plant_history_id"] = usina["last_plant_history_id"].astype(int)

    return usina

def clean_usina_historico(file_path):
    df = pd.read_csv(file_path)
    
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    
    df.fillna({'performance': 0}, inplace=True)

    df.dropna(subset=['plant_id', 'start_date'], inplace=True)
    
    df.drop(columns=['performance_type_enum'], inplace=True)
    
    return df


def clean_cidade_dask(file_path):
    dtype_dict = {
            'created_at': 'object',  # Ensure it's treated as a string initially
            'id_estado': 'int64',  # Define other columns as needed
            'id': 'int64'          # Adjust based on your dataset
        }

    # Load the CSV file into a Dask DataFrame
    df = dd.read_csv(file_path, dtype=dtype_dict)
    
    # Drop duplicate rows
    df = df.drop_duplicates()
    
    # Drop rows where 'id_estado' is NaN
    df = df.dropna(subset=['id_estado'])
    
    # Convert 'id_estado' and 'id' columns to integers
    df['id_estado'] = df['id_estado'].astype(int)
    df['id'] = df['id'].astype(int)

    df = df.drop(columns=['created_at'])  # Drop 'updated_at' column
    
    # Convert 'created_at' column to datetime, coercing errors

    return df


def clean_endereco(file_path):
    df = pd.read_csv(file_path)
    
    df.drop_duplicates(inplace=True)
    
    df.dropna(subset=['id_cidade'], inplace=True)
    
    df['id_cidade'] = df['id_cidade'].astype(int)
    df['id'] = df['id'].astype(int)

    
    return df


# Exemplo de uso
if __name__ == "__main__":
    # unidade_consumidora_clean = clean_unidade_consumidora("data/unidade_consumidora.csv")
    # unidade_consumidora_clean.to_csv("cleaned/unidade_consumidora.csv", index=False)

    # usina_clean = clean_usina("data/usina.csv")
    # usina_clean.to_csv("cleaned/usina.csv", index=False)

    geracao_clean = clean_geracao("data/geracao.csv")
    geracao_clean.to_csv("cleaned/geracao.csv", index=False)

    # usina_historico_clean = clean_usina_historico( "data/usina_historico.csv")
    # usina_historico_clean.to_csv("cleaned/usina_historico.csv", index=False)

    # cidade_clean = clean_cidade_dask("data/cidade.csv")
    # cidade_clean.to_csv("cleaned/cidade.csv", index=False, single_file=True)

    # endereco_clean = clean_endereco("data/endereco.csv")
    # endereco_clean.to_csv("cleaned/endereco.csv", index=False)