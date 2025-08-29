ELT-Bling: Plataforma de Dados de Vendas

## 🎯 Objetivo
Este projeto implementa um pipeline de dados (ELT) para extrair informações de pedidos de venda do ERP Bling. O objetivo é consolidar e transformar esses dados para viabilizar análises de negócio e tomadas de decisão estratégicas, sendo as principais:
- Análise de produtos curva ABC.
- Análise de lucratividade por produto.
- Estimar uma previsão da necessidade de reposição de estoque.
- Análise de performance por canal de venda.

## 🏗️ Arquitetura
O pipeline segue uma arquitetura moderna, utilizando ferramentas da Google Cloud Platform e o conceito de arquitetura Medallion para garantir a qualidade e a governança dos dados.

- Extração (Extract): Um job automatizado extrai os dados de pedidos da API do Bling de forma concorrente para maior performance.
- Carregamento (Load): Os dados brutos (raw) são carregados em um bucket no Google Cloud Storage (GCS), servindo como nossa camada Bronze (fonte de verdade).
- Transformação (Transform): O dbt é utilizado para transformar os dados armazenados no Google BigQuery, aplicando regras de negócio, limpeza e modelagem para criar as camadas Staging (colunas renomeadas), Silver (dados limpos e enriquecidos) e Gold (dados agregados e prontos para consumo).

<img width="4908" height="3346" alt="Arquitetura Pipeline" src="https://github.com/user-attachments/assets/4c54ed5a-cecb-4e72-9d31-73b5f5fb1455" />

## 🛠️ Tecnologias Utilizadas
- Extração: Python
- Transformação: dbt (Data Build Tool)
- Data Lake: Google Cloud Storage
- Data Warehouse: Google BigQuery
- Gerenciador de Segredos: Google Secret Manager
- Orquestração: Google Cloud Run Jobs
- Agendamento: Google Cloud Scheduler
- API: Bling API
- Containerização: Docker
