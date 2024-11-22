import pandas as pd

import matplotlib.pyplot as plt
import numpy as np


usina = pd.read_csv('data/usina.csv')
unidade_consumidora = pd.read_csv('data/unidade_consumidora.csv')
endereco = pd.read_csv('data/endereco.csv')
cidade = pd.read_csv('data/cidade.csv')

usina_unidade_consumidora = usina.merge(
    unidade_consumidora, 
    left_on='unidade_consumidora_id',
    right_on='id', 
    suffixes=('', '_unidade_consumidora'),
    how='inner',
)

usina_unidade_consumidora_endereco = usina_unidade_consumidora.merge(
    endereco,
    left_on='id_endereco',
    right_on='id',
    suffixes=('', '_endereco'),
    how='inner',
)


usina_unidade_consumidora_endereco_cidade = usina_unidade_consumidora_endereco.merge(
     cidade, 
     left_on='id_cidade', 
     right_on='id', 
     suffixes=('', '_cidade'), 
     how='inner',
     )

usina_unidade_consumidora_endereco_cidade.rename(columns={'nome': 'cidade'}, inplace=True)


usinas_por_cidade = (
    usina_unidade_consumidora_endereco_cidade
    .groupby('cidade')
    .size()
    .reset_index()
)

usinas_por_cidade = usinas_por_cidade.rename(columns={0: 'numero_usinas'})

usinas_por_cidade = usinas_por_cidade.head(10)

print(usinas_por_cidade.head())

# # Ordenar pelo número de usinas (opcional, para melhor visualização)
usinas_por_cidade = usinas_por_cidade.sort_values(by='numero_usinas', ascending=False)

# Criar o gráfico de barras
plt.figure(figsize=(12, 6))
plt.bar(usinas_por_cidade['cidade'], usinas_por_cidade['numero_usinas'], color='orange', alpha=0.7)

# Adicionar título e rótulos
plt.title('Número de Usinas por Cidade', fontsize=16)
plt.xlabel('Cidade', fontsize=12)
plt.ylabel('Número de Usinas', fontsize=12)

# # Rotacionar os rótulos do eixo X para legibilidade
plt.xticks(rotation=45, ha='right', fontsize=10)

# # Adicionar grid para melhor visualização
plt.grid(axis='y', linestyle='--', alpha=0.7)

# # Salvar o gráfico como imagem (opcional)
plt.savefig('numero_usinas_por_cidade.png', bbox_inches='tight')