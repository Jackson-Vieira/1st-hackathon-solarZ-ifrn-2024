import dask.dataframe as dd
from sklearn.ensemble import IsolationForest
import pandas as pd

# Carregar os dados como DataFrames Dask
geracao = dd.read_csv("data/geracao.csv")
usina = dd.read_csv("cleaned/usina.csv")
usina_historico = dd.read_csv("cleaned/usina_historico.csv")
unidade_consumidora = dd.read_csv("cleaned/unidade_consumidora.csv")

# -----


# Calcular a média de geração diária por planta
def calcular_media_geracao(geracao):
    result = (
        geracao.groupby("id_usina")["quantidade"]
        .mean()
        .reset_index()
        .rename(columns={"quantidade": "media_diaria"})
    )
    return result


media_geracao = calcular_media_geracao(geracao)
media_geracao.to_csv("debug/media_geracao.csv", index=False, single_file=True)

# -----


# Identificar a última entrada de histórico de cada usina
ultimo_historico = usina_historico.loc[
    usina_historico.groupby("plant_id")["start_date"].idxmax()
]

ultimo_historico = ultimo_historico.rename(
    columns={"power": "potencia_nominal", "plant_id": "id_usina"}
)

# -----


# Unir os dados de média de geração e potência nominal
usinas = media_geracao.merge(
    ultimo_historico[["id_usina", "potencia_nominal"]], on="id_usina"
)

usinas.to_csv("debug/media_geracao_potencia_nominal.csv", index=False, single_file=True)

# Passo 3: Relacionar com as regiões (endereço via unidade_consumidora)
usina = usina.merge(
    unidade_consumidora,
    left_on="unidade_consumidora_id",
    right_on="id",
    how="left",
    suffixes=("", "_unidade_consumidora"),
)

usina = usina.drop(columns=["unidade_consumidora_id", "id_unidade_consumidora"])

usina.to_csv("debug/usina_endereco.csv", index=False, single_file=True)

usinas = usinas.merge(
    usina[["id", "id_endereco"]], left_on="id_usina", right_on="id", how="left"
)

# Passo 4: Comparar a geração com outras usinas de mesma região e potência similar
# Normalizando potência para criar agrupamentos de faixas semelhantes

usinas["potencia_faixa"] = (usinas["potencia_nominal"] // 10) * 10  # Faixas de 10 kW

# Calculando a média esperada de geração por região e faixa de potência
media_regional = (
    usinas.groupby(["id_endereco", "potencia_faixa"])["media_diaria"]
    .mean()
    .reset_index()
    .rename(columns={"media_diaria": "media_regional"})
)

media_regional.to_csv("debug/media_regional.csv", index=False, single_file=True)

# Adicionar a média regional esperada aos dados das usinas
usinas = usinas.merge(media_regional, on=["id_endereco", "potencia_faixa"], how="left")

# Passo 5: Calcular a razão entre geração observada e esperada
usinas["proporcao_geracao"] = usinas["media_diaria"] / usinas["media_regional"]

# Remover NaNs
usinas = usinas.dropna()

usinas.to_csv("debug/usinas_proporcao_geracao.csv", index=False, single_file=True)

# Passo 6: Identificar anomalias usando Isolation Forest
# Para usar Isolation Forest com Dask, é necessário converter para Pandas temporariamente
# modelo_isolamento = IsolationForest(contamination=0.05, random_state=42)

# # Executar apenas em um DataFrame reduzido que o modelo consiga lidar
# usinas_pd = usinas.compute()  # Converte para Pandas para treinar o modelo
# usinas_pd["anomalia"] = modelo_isolamento.fit_predict(
#     usinas_pd[["proporcao_geracao", "potencia_nominal"]]
# )

# # Marcar usinas com anomalias (anomalia = -1)
# usinas_anomalas = usinas_pd[usinas_pd["anomalia"] == -1]

# # Exportar os resultados para análise
# usinas_anomalas.to_csv("usinas_anomalas.csv", index=False)

# print(
#     "Análise concluída. Usinas com possível problema exportadas para 'usinas_anomalas.csv'."
# )
