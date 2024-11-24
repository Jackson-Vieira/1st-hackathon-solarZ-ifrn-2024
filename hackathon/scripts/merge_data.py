from hackathon.utils.dataframe import load_data, clean_and_merge, rename_columns_with_prefix

usina = load_data("cleaned/usina.csv")
unidade_consumidora = load_data("cleaned/unidade_consumidora.csv")
usina_historico = load_data("cleaned/usina_historico.csv")
endereco = load_data("cleaned/endereco.csv")

cidade = load_data("cleaned/cidade.csv")
cidade = rename_columns_with_prefix(cidade, "cidade_")

usina_unidade_consumidora = clean_and_merge(usina, unidade_consumidora, "unidade_consumidora_id", "id", "_unidade_consumidora")

usina_unidade_consumidora = usina_unidade_consumidora.dropna(subset=["id_unidade_consumidora"])

usina_endereco = clean_and_merge(usina_unidade_consumidora, endereco, "id_endereco", "id", "_endereco_cidade")

usina_endereco = usina_endereco.dropna(subset=["id_endereco_cidade"])

usina_cidade = clean_and_merge(usina_endereco, cidade, "id_cidade", "cidade_id", "")

usina = usina_cidade.dropna(subset=["id_cidade"])

columns_to_drop = ["id_unidade_consumidora", "id_endereco", "id_cidade", "cidade_id", "id_endereco_cidade", "cidade_id_estado"]
usina = usina.drop(columns=columns_to_drop)

usina.to_csv("cleaned/usina_merged.csv", index=False, single_file=True)