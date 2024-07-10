import pandas as pd
import os
import shutil

# Defina o caminho da pasta de entrada e de processados
pasta_entrada = r'C:\Files\Leitura\Dados\5RM3\Fases'
pasta_processados = r'C:\Files\Leitura\Dados\5RM3\Fases\PROCESSADOS'

# Caminho do arquivo antigo
arquivo_antigo = r'C:\Files\INDICADORES DE LEITURAS - COPASA\DATABASE\indicadores_leitura_antigo.csv'

# Leia o arquivo antigo no DataFrame
df_total = pd.read_csv(arquivo_antigo, delimiter=";")

# Lista todos os arquivos .txt na pasta de entrada
arquivos = [f for f in os.listdir(pasta_entrada) if f.endswith('.txt')]

for arquivo in arquivos:
    # Construa o caminho completo do arquivo
    caminho_arquivo = os.path.join(pasta_entrada, arquivo)
    
    # Leia o arquivo .txt no DataFrame
    df = pd.read_csv(caminho_arquivo, delimiter=";", header=0)
    
    # Selecione as colunas necessárias e renomeie a coluna 'OCORRENCIA'
    df = df[["REFER", "UNIDADE", "GERENCIA", "NOME-LOCALIDADE", "OCORRENCIA"]].rename(columns={"OCORRENCIA": "COD_OCORRENCIA"})
    
    # Remova os espaços em branco no início e fim das colunas
    df = df.apply(lambda x: x.map(str.strip) if x.dtype == "object" else x)
    
    # Agregue os valores da contagem da coluna 'COD_OCORRENCIA'
    df = df.groupby(["REFER", "UNIDADE", "GERENCIA", "NOME-LOCALIDADE", "COD_OCORRENCIA"]).size().reset_index(name="QTD_LEITURAS")

    df = df[["REFER", "UNIDADE", "GERENCIA", "NOME-LOCALIDADE", "QTD_LEITURAS", "COD_OCORRENCIA"]]

    # Adicione os dados ao DataFrame total
    df_total = pd.concat([df_total, df])
    
    # Mova o arquivo .txt processado para a pasta 'Processados'
    #shutil.move(caminho_arquivo, os.path.join(pasta_processados, arquivo))

    print(f'{caminho_arquivo} concluído!')

df_total = df_total.sort_values('REFER')

# Escreva o DataFrame total em um arquivo .parquet
df_total.to_parquet(r'C:\Files\INDICADORES DE LEITURAS - COPASA\DATABASE\indicadores_leituras.parquet', index=False)

# Escreva o DataFrame total em um arquivo .csv
df_total.to_csv(r'C:\Files\INDICADORES DE LEITURAS - COPASA\DATABASE\indicadores_leituras.csv', index=False)

print("Tarefa Concluída!")
