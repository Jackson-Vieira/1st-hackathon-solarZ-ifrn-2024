import dask.dataframe as dd
from utils.convert import dataframe_to_parquet
from dask.diagnostics import ProgressBar
import logging

from tqdm.dask import TqdmCallback


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)


if __name__ == "__main__":
    logging.info("Iniciando a leitura do arquivo CSV...")

    df = dd.read_csv("data/geracao.csv")
    logging.info(f"Leitura concluída. Dataframe contém {len(df.columns)} colunas.")

    num_partitions = df.npartitions
    logging.info(f"O dataframe foi dividido em {num_partitions} partições para processamento.")

    with TqdmCallback(desc="compute", total_tasks=num_partitions) as progress, ProgressBar():
        logging.info("Iniciando a conversão do dataframe para Parquet...")
        dataframe_to_parquet(df, "cleaned/geracao")
        # df.compute()
    

    logging.info("Conversão para Parquet concluída com sucesso!")