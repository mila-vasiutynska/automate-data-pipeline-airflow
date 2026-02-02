from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="redshift",
        table="",
        sql="",
        truncate_before_insert=True,
        *args,
        **kwargs
    ):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate_before_insert = truncate_before_insert

    def execute(self, context):
        self.log.info("Loading dimension table %s", self.table)

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_before_insert:
            self.log.info("Truncating dimension table %s before insert", self.table)
            redshift.run("TRUNCATE TABLE {}".format(self.table))

        insert_sql = """
            INSERT INTO {table}
            {sql}
        """.format(
            table=self.table,
            sql=self.sql
        )

        redshift.run(insert_sql)

        self.log.info("LoadDimensionOperator finished for table %s", self.table)
