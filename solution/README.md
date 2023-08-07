Este repositório contém o código para um pipeline ETL que extrai dados de vendas de combustíveis da Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP). O pipeline extrai os dados de um arquivo xlsx e os transforma em um formato que pode ser usado para análise. Os dados transformados são então salvos em um arquivo Parquet comprimido.

### Instalação
Para instalar o pipeline, você precisará instalar as seguintes dependências:

Python 3.6+
Pandas
OpenPyXL
Apache Airflow
Docker
Depois de instalar as dependências, você pode clonar o repositório e executar o pipeline usando o seguinte comando:

docker-compose up -d


### Uso
O pipeline pode ser usado para extrair e transformar os dados de vendas de combustíveis da ANP. Os dados transformados podem ser usados para análise, relatórios ou visualização.

Eu também defini um esquema de particionamento conveniente para os dados. Os dados são particionados por ano e mês. Isso torna mais fácil pesquisar e analisar os dados.

Para verificar se os dados extraídos são consistentes com os valores consolidados nas tabelas brutas, adicionei uma etapa ao pipeline para verificar a soma dos volumes. Se a soma dos volumes não for igual à soma dos volumes nas tabelas brutas, o pipeline falhará.