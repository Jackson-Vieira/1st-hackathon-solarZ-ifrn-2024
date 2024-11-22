import dask.dataframe as dd
from matplotlib import pyplot as plt
# import sklearn.ensemble import IsolationForest

# carregar dados
usina = dd.read_csv("cleaned/usina.csv")
unidade_consumidora = dd.read_csv("cleaned/unidade_consumidora.csv")
usina_historico = dd.read_csv("cleaned/usina_historico.csv")
endereco = dd.read_csv("cleaned/endereco.csv")
estado = dd.read_csv("cleaned/estado.csv")
cidade = dd.read_csv("cleaned/cidade.csv")


usina = usina.merge(
    unidade_consumidora,
    left_on="unidade_consumidora_id",
    right_on="id",
    how="left",
    suffixes=("", "_unidade_consumidora"),
)

# clean usina if not have unidade_consumidora_id
usina = usina[usina["id_unidade_consumidora"].notnull()]

usina = usina.merge(
    endereco,
    left_on="id_endereco",
    right_on="id",
    how="left",
    suffixes=("", "_endereco_cidade"),
)

print(usina.head())

usina = usina[usina["id_endereco_cidade"].notnull()]

# pre process cidade and rename all columns to prefix with cidade_
cidade = cidade.rename(columns={col: f"cidade_{col}" for col in cidade.columns})


usina = usina.merge(
    cidade,
    left_on="id_cidade",
    right_on="cidade_id",
    how="left",
)

estado = estado.rename(columns={col: f"estado_{col}" for col in estado.columns})

usina = usina.merge(
    estado,
    left_on="cidade_id_estado",
    right_on="estado_id",
    how="left",
    suffixes=("", "_estado"),
)

usina = usina.drop(columns=["id_unidade_consumidora", "id_endereco", "id_cidade", "cidade_id", "cidade_id_estado", "estado_id", "estado_denominacao", "estado_regiao"])

print(usina.head())
