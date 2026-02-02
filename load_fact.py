from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = "#F98866"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="redshift",
        table="",
        sql="",
        insert_columns=None,   
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.insert_columns = insert_columns or []  

    def execute(self, context):
        self.log.info("Loading fact table %s", self.table)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        cols = f"({', '.join(self.insert_columns)})" if self.insert_columns else ""
        insert_sql = f"INSERT INTO {self.table} {cols}\n{self.sql}"

        redshift.run(insert_sql)
        self.log.info("LoadFactOperator finished for table %s", self.table)
