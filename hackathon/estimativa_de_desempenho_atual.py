import dask.dataframe as dd

usina = dd.read_csv("cleaned/usina.csv")
unidade_consumidora = dd.read_csv("cleaned/unidade_consumidora.csv")
usina_historico = dd.read_csv("cleaned/usina_historico.csv")
endereco = dd.read_csv("cleaned/endereco.csv")
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

usina = usina[usina["id_endereco_cidade"].notnull()]

cidade = cidade.rename(columns={col: f"cidade_{col}" for col in cidade.columns})

usina = usina.merge(
    cidade,
    left_on="id_cidade",
    right_on="cidade_id",
    how="left",
)


usina = usina.drop(columns=["id_unidade_consumidora", "id_endereco", "id_cidade", "cidade_id"])

print(usina.head())
