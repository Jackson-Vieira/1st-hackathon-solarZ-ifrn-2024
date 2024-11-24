from hackathon.utils.dataframe import load_data, clean_and_merge, rename_columns_with_prefix

usina = load_data("cleaned/usina.csv")
unidade_consumidora = load_data("cleaned/unidade_consumidora.csv")
usina_historico = load_data("cleaned/usina_historico.csv")
endereco = load_data("cleaned/endereco.csv")

cidade = load_data("cleaned/cidade.csv")
cidade = rename_columns_with_prefix(cidade, "cidade_")

usina_unidade_consumidora = clean_and_merge(usina, unidade_consumidora, "unidade_consumidora_id", "id", "_unidade_consumidora")
usina_unidade_consumidora = usina.dropna(subset=["id_unidade_consumidora"])

usina_endereco = clean_and_merge(usina, endereco, "id_endereco", "id", "_endereco_cidade")
usina_endereco = usina.dropna(subset=["id_endereco_cidade"])

usina = usina.merge(
    cidade,
    left_on="id_cidade",
    right_on="cidade_id",
    how="left",
)

usina = usina.dropna(subset=["cidade_id"])

columns_to_drop = ["id_unidade_consumidora", "id_endereco", "id_cidade", "cidade_id", "id_endereco_cidade", "cidade_id_estado"]
usina = usina.drop(columns=columns_to_drop)

usina.to_csv("cleaned/usina_merged.csv", index=False, single_file=True)