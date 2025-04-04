import numpy as np
import pandas as pd

import dask.dataframe as dd

def clean_geracao(geracao_path):
    geracao = dd.read_csv(geracao_path)
    geracao = geracao.dropna(subset=["id_usina"])
    geracao["id_usina"] = geracao["id_usina"].astype(int)
    return geracao

def clean_unidade_consumidora(file_path: str):
    """
    Limpa o arquivo de unidade consumidora, removendo registros duplicados e registros sem id_endereco
    """

    unidade_consumidora = pd.read_csv(file_path)

    # unidade_consumidora.drop_duplicates(inplace=True)
    unidade_consumidora = unidade_consumidora.dropna(subset=["id_endereco"])
    unidade_consumidora["id_endereco"] = unidade_consumidora["id_endereco"].astype(int)

    return unidade_consumidora

def clean_usina(file_path: str):
    """
    Limpa o arquivo de usina, removendo registros duplicados e registros sem last_plant_history_id 
    """

    usina = pd.read_csv(file_path)

    usina.drop_duplicates(inplace=True)
    usina = usina.dropna(subset=["last_plant_history_id"])

    # converter last_plant_history_id para inteiro
    usina["last_plant_history_id"] = usina["last_plant_history_id"].astype(int)

    return usina

def clean_usina_historico(file_path):
    """
    Limpa o arquivo de histórico de usina, removendo registros duplicados e registros sem plant_id e start_date 
    """

    df = pd.read_csv(file_path)
    
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    
    df.fillna({'performance': 0}, inplace=True)

    df.dropna(subset=['plant_id', 'start_date'], inplace=True)
    
    df.drop(columns=['performance_type_enum'], inplace=True)
    
    return df


def clean_cidade_dask(file_path):
    """
    Limpa o arquivo de cidade, removendo registros duplicados e registros sem id_estado 
    remover coluna created_at
    """

    df = df.read_csv(file_path)
    
    df = df.drop_duplicates()
    df = df.dropna(subset=['id_estado'])
    
    df['id_estado'] = df['id_estado'].astype(int)
    df['id'] = df['id'].astype(int)

    df = df.drop(columns=['created_at'])
    
    return df


def clean_endereco(file_path):
    """
    Limpa o arquivo de endereço, removendo registros duplicados e registros sem id_cidade 
    """
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

    # geracao_clean = clean_geracao("data/geracao.csv")
    # geracao_clean.to_csv("cleaned/geracao.csv", index=False)

    # usina_historico_clean = clean_usina_historico( "data/usina_historico.csv")
    # usina_historico_clean.to_csv("cleaned/usina_historico.csv", index=False)

    # cidade_clean = clean_cidade_dask("data/cidade.csv")
    # cidade_clean.to_csv("cleaned/cidade.csv", index=False, single_file=True)

    # endereco_clean = clean_endereco("data/endereco.csv")
    # endereco_clean.to_csv("cleaned/endereco.csv", index=False)