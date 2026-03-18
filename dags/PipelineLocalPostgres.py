import pendulum
import pandas as pd

from airflow import DAG
from airflow.datasets import Dataset
from airflow.sdk import task 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import (
    SQLTableCheckOperator,
    SQLColumnCheckOperator,
)
from airflow.operators.email import EmailOperator

# Configurações

DATA_PATH = "/opt/airflow/data/producao_alimentos.csv"
PARQUET_PATH = "/opt/airflow/data/producao_trusted.parquet"
TABLE_NAME = "producao"
POSTGRES_CONN_ID = "postgres_default"

dataset = Dataset(DATA_PATH)

# Funções de domínio

def calculate_profit_margin(df: pd.DataFrame) -> pd.Series:
    return (
        (df["receita_total"] / df["quantidade_produzida_kgs"])
        - df["valor_venda_medio"]
    ).round(2)

# DAG

with DAG(
    dag_id="pipeline_producao_postgres",
    description="Pipeline ETL com Pandas + Parquet + PostgreSQL",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["etl", "pandas", "parquet", "postgres"],
) as dag:

    #email alerta de falha com informações detalhadas
    email_alert = EmailOperator(
        task_id="email_alert",
        to=["contato@evoluth.com.br"],
        subject="🚨 Falha na DAG {{ dag.dag_id }}",
        html_content="""
            <h3>Falha na execução</h3>
            <p><b>DAG:</b> {{ dag.dag_id }}</p>
            <p><b>Task:</b> {{ ti.task_id }}</p>
            <p><b>Execution Date:</b> {{ ds }}</p>
            <p>Verifique os logs no Airflow.</p>
        """,
        trigger_rule="one_failed",
    )

    #validação pré-load
    @task
    def validate_data(parquet_path: str) -> str:
        df = pd.read_parquet(parquet_path)

        # 1. não pode estar vazio
        if df.empty:
            raise ValueError("Dataset vazio após transformação")

        # 2. nulls
        if df["produto"].isnull().any():
            raise ValueError("Produto contém valores nulos")

        # 3. valores negativos
        if (df["quantidade_produzida_kgs"] <= 0).any():
            raise ValueError("Quantidade inválida")

        if (df["valor_venda_medio"] < 0).any():
            raise ValueError("Preço médio inválido")

        if (df["receita_total"] < 0).any():
            raise ValueError("Receita inválida")

        # 4. consistência básica
        if (df["receita_total"] < df["valor_venda_medio"] * df["quantidade_produzida_kgs"] * 0.1).any():
            raise ValueError("Receita inconsistente (possível erro de parsing)")

        print(f"Validação pré-load OK")

        return parquet_path

    #Garante que a tabela não está vazia.
    chk_producao_tem_dados = SQLTableCheckOperator(
        task_id="chk_producao_tem_dados",
        conn_id=POSTGRES_CONN_ID,
        table=TABLE_NAME,
        checks={
            "row_count_nonzero": {
                "check_statement": "COUNT(*) > 0"
            }
        },
    )

    #valores nulos, valores negativos
    chk_colunas_producao = SQLColumnCheckOperator(
        task_id="chk_colunas_producao",
        conn_id=POSTGRES_CONN_ID,
        table=TABLE_NAME,
        column_mapping={
            "produto": {
                "null_check": {"equal_to": 0}
            },
            "quantidade_produzida_kgs": {
                "null_check": {"equal_to": 0},
                "min": {"geq_to": 0}
            },
            "valor_venda_medio": {
                "null_check": {"equal_to": 0},
                "min": {"geq_to": 0}
            },
            "receita_total": {
                "null_check": {"equal_to": 0},
                "min": {"geq_to": 0}
            },
            "margem_lucro": {
                "null_check": {"equal_to": 0}
            },
        },
    )

    # Garante que não existe divisão por zero ou inválida
    chk_regra_margem = SQLTableCheckOperator(
        task_id="chk_regra_margem",
        conn_id=POSTGRES_CONN_ID,
        table=TABLE_NAME,
        checks={
            "margem_valida": {
                "check_statement": """
                    NOT EXISTS (
                        SELECT 1
                        FROM producao
                        WHERE quantidade_produzida_kgs = 0
                    )
                """
            }
        },
    )

    # Detecta valores absurdos / inconsistentes
    chk_receita_consistente = SQLTableCheckOperator(
        task_id="chk_receita_consistente",
        conn_id=POSTGRES_CONN_ID,
        table=TABLE_NAME,
        checks={
            "receita_coerente": {
                "check_statement": """
                    NOT EXISTS (
                        SELECT 1
                        FROM producao
                        WHERE receita_total < valor_venda_medio * quantidade_produzida_kgs * 0.5
                    )
                """
            }
        },
    )

    @task
    def process_data() -> str:
        df = pd.read_csv(
            DATA_PATH,
            decimal=',',
            thousands='.'
        )

        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
        )

        df = df[df["quantidade_produzida_kgs"] > 10]

        df["margem_lucro"] = calculate_profit_margin(df)

        df.to_parquet(PARQUET_PATH, index=False)

        return PARQUET_PATH

    @task
    def load_to_postgres(parquet_path: str):
        df = pd.read_parquet(parquet_path)

        hook = PostgresHook(postgres_conn_id="postgres_default")
        engine = hook.get_sqlalchemy_engine()

        df.to_sql(
            name=TABLE_NAME,
            con=engine,
            if_exists="replace",
            index=False
        )

    parquet_file = process_data()

    validated_file = validate_data(parquet_file)

    load = load_to_postgres(validated_file)

    load >> [
        chk_producao_tem_dados,
        chk_colunas_producao,
        chk_regra_margem,
        chk_receita_consistente
    ] >> email_alert