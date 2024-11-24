from hackathon.utils.dataframe import load_data

# Load data
usina = load_data("cleaned/usina.csv")
unidade_consumidora = load_data("cleaned/unidade_consumidora.csv")
usina_historico = load_data("cleaned/usina_historico.csv")
endereco = load_data("cleaned/endereco.csv")
cidade = load_data("cleaned/cidade.csv")

# Merge data
usina = clean_and_merge(
    usina, unidade_consumidora, "unidade_consumidora_id", "id", "_unidade_consumidora"
)

# Remover usinas sem unidade consumidora
usina = usina.dropna(subset=["id_unidade_consumidora"])

usina = clean_and_merge(
    usina, endereco, "id_endereco", "id", "_endereco_cidade"
)

# Remover usinas sem endereco
usina = usina.dropna(subset=["id_endereco_cidade"])

cidade = rename_columns_with_prefix(cidade, "cidade_")

usina = usina.merge(
    cidade,
    left_on="id_cidade",
    right_on="cidade_id",
    how="left",
)

# Remover usinas sem cidade

usina = usina.dropna(subset=["cidade_id"])


columns_to_drop = ["id_unidade_consumidora", "id_endereco", "id_cidade", "cidade_id", "id_endereco_cidade", "cidade_id_estado"]
usina = usina.drop(columns=columns_to_drop)

usina.to_csv("cleaned/usina_merged.csv", index=False, single_file=True)