# dados_5rm3_pandas
 Modelagem de dados com Pandas para criação de arquivo .parquet


Aqui está um exemplo de como poderia ser o `README.md` para o seu código:

```markdown
# Processamento de Arquivos TXT e Atualização de Arquivo CSV com Pandas

Este repositório contém um script Python que utiliza Pandas para processar arquivos `.txt` de uma pasta, realizar transformações e agregações nos dados, e atualizar um arquivo CSV existente. Os resultados finais são salvos em formatos `.parquet` e `.csv`.

## Requisitos

- Python 3.x
- Pandas
- Os pacotes `os` e `shutil` (nativos do Python)

## Instalação

1. Clone o repositório:
   ```bash
   git clone https://github.com/seu-usuario/seu-repositorio.git
   ```
2. Navegue até o diretório do projeto:
   ```bash
   cd seu-repositorio
   ```
3. Instale as dependências necessárias:
   ```bash
   pip install pandas
   ```

## Uso

1. Defina os caminhos das pastas de entrada, de processados e do arquivo antigo no script conforme necessário.
2. Coloque seus arquivos `.txt` na pasta de entrada especificada.
3. Execute o script:
   ```bash
   python seu_script.py
   ```

## Descrição do Script

O script realiza as seguintes operações:

1. **Definição dos Caminhos das Pastas**:
   ```python
   pasta_entrada = r'C:\Files\Leitura\Dados\5RM3\Fases'
   pasta_processados = r'C:\Files\Leitura\Dados\5RM3\Fases\PROCESSADOS'
   ```

2. **Definição do Caminho do Arquivo Antigo**:
   ```python
   arquivo_antigo = r'C:\Files\INDICADORES DE LEITURAS - COPASA\DATABASE\indicadores_leitura_antigo.csv'
   ```

3. **Leitura do Arquivo Antigo no DataFrame**:
   ```python
   df_total = pd.read_csv(arquivo_antigo, delimiter=";")
   ```

4. **Listagem dos Arquivos `.txt` na Pasta de Entrada**:
   ```python
   arquivos = [f for f in os.listdir(pasta_entrada) if f.endswith('.txt')]
   ```

5. **Processamento de Cada Arquivo**:
   - Construção do caminho completo do arquivo:
     ```python
     caminho_arquivo = os.path.join(pasta_entrada, arquivo)
     ```
   - Leitura do arquivo `.txt` no DataFrame:
     ```python
     df = pd.read_csv(caminho_arquivo, delimiter=";", header=0)
     ```
   - Seleção e renomeação das colunas:
     ```python
     df = df[["REFER", "UNIDADE", "GERENCIA", "NOME-LOCALIDADE", "OCORRENCIA"]].rename(columns={"OCORRENCIA": "COD_OCORRENCIA"})
     ```
   - Remoção dos espaços em branco no início e fim das colunas:
     ```python
     df = df.apply(lambda x: x.map(str.strip) if x.dtype == "object" else x)
     ```
   - Agregação dos valores da contagem da coluna `COD_OCORRENCIA`:
     ```python
     df = df.groupby(["REFER", "UNIDADE", "GERENCIA", "NOME-LOCALIDADE", "COD_OCORRENCIA"]).size().reset_index(name="QTD_LEITURAS")
     ```
   - Seleção das colunas finais:
     ```python
     df = df[["REFER", "UNIDADE", "GERENCIA", "NOME-LOCALIDADE", "QTD_LEITURAS", "COD_OCORRENCIA"]]
     ```
   - Adição dos dados ao DataFrame total:
     ```python
     df_total = pd.concat([df_total, df])
     ```
   - (Opcional) Movimentação do arquivo `.txt` processado para a pasta 'Processados':
     ```python
     shutil.move(caminho_arquivo, os.path.join(pasta_processados, arquivo))
     ```
   - Impressão de mensagem de conclusão para cada arquivo:
     ```python
     print(f'{caminho_arquivo} concluído!')
     ```

6. **Ordenação do DataFrame Total pela Coluna 'REFER'**:
   ```python
   df_total = df_total.sort_values('REFER')
   ```

7. **Escrita do DataFrame Total em um Arquivo `.parquet`**:
   ```python
   df_total.to_parquet(r'C:\Files\INDICADORES DE LEITURAS - COPASA\DATABASE\indicadores_leituras.parquet', index=False)
   ```

8. **Escrita do DataFrame Total em um Arquivo `.csv`**:
   ```python
   df_total.to_csv(r'C:\Files\INDICADORES DE LEITURAS - COPASA\DATABASE\indicadores_leituras.csv', index=False)
   ```

9. **Mensagem de Conclusão Geral**:
   ```python
   print("Tarefa Concluída!")
   ```

## Contribuição

1. Faça um fork do projeto
2. Crie uma nova branch (`git checkout -b feature/nova-funcionalidade`)
3. Commit suas mudanças (`git commit -am 'Adiciona nova funcionalidade'`)
4. Envie para a branch (`git push origin feature/nova-funcionalidade`)
5. Crie um novo Pull Request

## Licença

Este projeto está licenciado sob a Licença MIT. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.
```
