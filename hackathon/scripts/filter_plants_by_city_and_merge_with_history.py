from hackathon.utils.dataframe import load_data 

CIDADE = "Natal"
FILE_PATH = f"cleaned/usina_{CIDADE.lower()}_merged.csv"

usinas = load_data("cleaned/usina_merged.csv")
usinas_historico = load_data("cleaned/usina_historico.csv")

usinas_filtradas = usinas[usinas['cidade_nome'] == CIDADE]

ids_usinas_filtradas = usinas_filtradas['id']

historico_usinas_filtradas = usinas_historico[usinas_historico['plant_id'].isin(ids_usinas_filtradas)]

historico_usinas_filtradas = historico_usinas_filtradas.dropna(subset=['plant_id'])

historico_usinas_sorted = historico_usinas_filtradas.sort_values(by=['plant_id', 'start_date'])

# TODO: Testar se isto está realmente preciso
current_historico_usinas = historico_usinas_sorted.groupby('plant_id').last().reset_index()

potencia_atual = current_historico_usinas.rename(columns={'power': 'current_power', 'start_date': 'last_update'})

usina_com_potencia_atual = usinas_filtradas.merge(
    potencia_atual[['plant_id', 'current_power', 'last_update']],
    left_on='id',
    right_on='plant_id',
    how='left'
)

total_usinas_cidade_merged = usina_com_potencia_atual.shape[0].compute()

print(usina_com_potencia_atual.head())
print(f"Total de usinas em {CIDADE}: {total_usinas_cidade_merged}")

# usina_com_potencia_atual.to_csv(FILE_PATH, index=False, single_file=True)