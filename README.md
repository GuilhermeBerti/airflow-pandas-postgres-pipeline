# 🚀 Pipeline de Dados com Apache Airflow, Pandas, Parquet e PostgreSQL

Este projeto implementa um pipeline de dados completo seguindo boas práticas de Engenharia de Dados, utilizando **Apache Airflow** para orquestração, **Pandas** para transformação, **Parquet** como camada intermediária (Silver) e **PostgreSQL** como camada analítica (Gold).

---

## 📌 Arquitetura do Projeto

O pipeline segue o padrão moderno de **Data Lake em camadas (Medallion Architecture)**:

```
Bronze (Raw)      → CSV
Silver (Trusted)  → Parquet
Gold (Analytics)  → PostgreSQL
```

### 🔄 Fluxo do Pipeline

```
CSV → Transformação → Validação → Load → Validações SQL → Alertas
```

---

## 🧠 Tecnologias Utilizadas

* Apache Airflow
* Python (Pandas)
* PostgreSQL
* Parquet
* Docker (opcional)
* SQL (validações analíticas)

---

## 📂 Estrutura do Projeto

```
.
├── dags/
│   └── pipeline_producao_postgres.py
├── data/
│   ├── producao_alimentos.csv        # Bronze
│   └── producao_trusted.parquet      # Silver
├── logs/
├── plugins/
└── README.md
```

---

## ⚙️ Funcionalidades

### ✅ 1. Ingestão de Dados (Bronze)

* Leitura de arquivo CSV
* Tratamento de separadores (`decimal=','`, `thousands='.'`)
* Padronização de colunas

---

### 🔄 2. Transformação (Silver)

* Filtro de dados inválidos
* Cálculo de métricas de negócio:

  * Margem de lucro
* Escrita em formato **Parquet (otimizado)**

---

### 🧪 3. Validação Pré-Load (Data Quality)

Antes de carregar no banco:

* Dataset não pode estar vazio
* Campos obrigatórios não podem ser nulos
* Valores negativos inválidos
* Consistência de receita (evita erro de parsing)

---

### 🛢️ 4. Carga no PostgreSQL (Gold)

* Persistência dos dados tratados
* Substituição da tabela (`replace`)
* Uso de `SQLAlchemy` via `PostgresHook`

---

### 📊 5. Validações Pós-Load (SQL)

Executadas diretamente no banco:

* ✔ Tabela não vazia
* ✔ Colunas sem valores nulos
* ✔ Valores numéricos válidos
* ✔ Regra de margem (sem divisão por zero)
* ✔ Consistência da receita

---

### 🚨 6. Alertas Automáticos

* Envio de e-mail em caso de falha
* Monitoramento de todo o pipeline
* Uso de `EmailOperator` com `trigger_rule="one_failed"`

---

## 🧩 DAG Overview

```python
process_data 
    >> validate_data 
    >> load_to_postgres 
    >> [checks SQL em paralelo] 
    >> email_alert
```

---

## ▶️ Como Executar

### 1. Subir o Airflow

Se estiver usando Docker:

```bash
docker-compose up -d
```

---

### 2. Acessar o Airflow

```
http://localhost:8080
```

---

### 3. Configurar conexão com PostgreSQL

* Conn ID: `postgres_default`
* Host: `postgres`
* Porta: `5432`
* Usuário/Senha conforme ambiente

---

### 4. Executar a DAG

* Ative a DAG: `pipeline_producao_postgres`
* Execute manualmente ou via trigger

---

## 📈 Boas Práticas Aplicadas

* ✔ Separação em camadas (Bronze / Silver / Gold)
* ✔ Validação de dados antes e depois do load
* ✔ Pipeline idempotente
* ✔ Uso de Parquet para performance
* ✔ Orquestração com Airflow
* ✔ Observabilidade com alertas
* ✔ Código limpo e modular
* ✔ Regras de negócio explícitas

---

## 🧠 Regras de Negócio Implementadas

### 💰 Margem de Lucro

```python
(receita_total / quantidade_produzida_kgs) - valor_venda_medio
```

---

### ⚠️ Consistência de Receita

Evita erros como:

```
16500 → 165 (problema de parsing)
```

---

## 🚀 Possíveis Melhorias

* 🔹 Particionamento de dados no Parquet
* 🔹 Incremental load
* 🔹 Integração com Slack para alertas
* 🔹 Uso de Delta Lake
* 🔹 Data Quality com Great Expectations
* 🔹 Monitoramento com métricas (DataOps)

---

## 💬 Sobre o Projeto

Este projeto foi desenvolvido com foco em demonstrar habilidades práticas em:

* Engenharia de Dados
* Construção de pipelines robustos
* Validação e qualidade de dados
* Boas práticas de arquitetura

---

## 👨‍💻 Autor

**Guilherme Berti**
Engenheiro de Dados

* GitHub: https://github.com/GuilhermeBerti
* Portfólio: https://GuilhermeBerti.github.io

---

## 📜 Licença

Este projeto é livre para uso educacional e profissional.
