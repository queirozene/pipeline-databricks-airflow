# 🏗️ pipeline-databricks-airflow

Este repositório faz parte de um estudo prático que implementa um pipeline de ETL utilizando **Apache Airflow**, **Azure Databricks** e integração com **Slack**, com o objetivo de extrair, transformar e reportar dados de câmbio.

> ⚠️ **Aviso:** Este projeto está hospedado como portfólio e depende de conexões configuradas (Databricks e Slack) para execução completa.

---

## 💡 Objetivo

Construir um pipeline de dados que:

1. **Extrai dados de câmbio** diariamente através da API Exchange Rates (USD, EUR, GBP em relação ao BRL).
2. Realiza o **processamento e padronização** dos dados com notebooks no Databricks.
3. **Gera e envia relatórios automatizados** com dados e gráficos via Slack, todos os dias às 9h.

---

## 🛠️ Tecnologias e Ferramentas Utilizadas

- **Apache Airflow**: Orquestração das tarefas via DAGs.
- **Azure Databricks**: Execução de notebooks em PySpark.
- **Slack API**: Envio automatizado de relatórios.
- **Python + PySpark**
- **DBFS (Databricks File System)**: Armazenamento intermediário dos dados.
- **Linux com Virtualenv**: Ambiente de desenvolvimento.

---

## 📁 Estrutura do Repositório

```
├── dags/                       # DAGs utilizadas no Airflow
│   └── extract-tax.py
├── notebooks/                  # Notebooks executados no Databricks
│   ├── 1-extract-data.py
│   ├── 2-data-transform.py
|   ├── 2.1-data-transform-to-pandas.py # arquivo teste não utilizado no job, só convertido o tratamento para pandas spark
│   └── 3-automating-report.py
├── imagens/                    # Gráficos gerados e enviados via Slack
│   ├── USD.png
│   ├── EUR.png
│   └── GBP.png
├── requirements.txt            # Dependências do projeto
├── README.md                   # Este arquivo
└── .gitignore
```

---

## 🔄 Fluxo do Pipeline

1. A **DAG no Airflow** é agendada para rodar todos os dias às 09:00.
2. A tarefa `extract_data` extrai dados de câmbio da API e salva no DBFS (camada Bronze).
3. A tarefa `transform_data` realiza padronizações e conversões, salvando na camada Silver.
4. A tarefa `send_report`:
   - Gera gráficos de evolução para cada moeda (USD, EUR, GBP).
   - Envia para o Slack uma tabela com os valores tratados e os gráficos correspondentes.

---

## 📌 Observações

- Os gráficos são gerados dinamicamente e salvos na pasta `imagens/` antes de serem enviados.
- A integração com o Slack utiliza tokens pessoais e webhooks (não incluídos no repositório).
- Cada notebook é executado via `DatabricksRunNowOperator`, referenciando o `job_id` correspondente para cada JOB no Databricks.

---

## 👤 Autor

- [Gustavo Ribeiro](https://github.com/queirozene)
- [LinkedIn](https://www.linkedin.com/in/guxtavoqr/)
