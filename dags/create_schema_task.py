from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# Class PostgresOperators (đã được cung cấp trước đó)
class PostgresOperators:
    def __init__(self, conn_id):
        self.conn_id = conn_id
        self.hook = PostgresHook(postgres_conn_id=self.conn_id)

    def execute_query(self, sql):
        self.hook.run(sql)

# Hàm tạo schema nếu chưa tồn tại
def create_schema_if_not_exists():
    postgres_operator = PostgresOperators(conn_id="postgres")
    create_schema_query = "CREATE SCHEMA IF NOT EXISTS warehouse;"
    postgres_operator.execute_query(create_schema_query)
