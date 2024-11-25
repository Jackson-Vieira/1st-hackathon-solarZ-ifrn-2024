import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

df = pd.read_csv("output/usinas_grupo.csv")

filtered_df = df[df['power_range'] == "(15.0, 20.0]"]

sns.set(style="whitegrid")

plt.figure(figsize=(10, 6))
scatter = sns.scatterplot(
    x="current_power",
    y="quantidade",
    hue="anomalous", 
    palette={True: "red", False: "blue"},
    data=filtered_df,
    s=100
)

plt.xlim(0, 21)
plt.ylim(0, 100)

# Títulos e rótulos
plt.title("Dispersão de Usinas no Range (15.0, 20.0] de Potência", fontsize=16)
plt.xlabel("Potência Instalada (kW)", fontsize=14)
plt.ylabel("Produção Média (kWh)", fontsize=14)

# Legenda personalizada
plt.legend(title="Anômalo", loc="upper left", fontsize=12)

# Exibir o gráfico
plt.show()