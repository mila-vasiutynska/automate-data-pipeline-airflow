from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="redshift",
        aws_conn_id="aws_credentials",
        table="",
        s3_bucket="",
        s3_key="",
        json_path="auto",
        region="us-east-1",
        timeformat=None,          # ✅ NEW
        truncate=True,            # ✅ NEW (optional)
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.region = region
        self.timeformat = timeformat
        self.truncate = truncate

    def execute(self, context):
        aws_hook = AwsBaseHook(aws_conn_id=self.aws_conn_id, client_type="sts")
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        self.log.info("Staging from %s into Redshift table %s", s3_path, self.table)

        if self.truncate:
            redshift.run(f"TRUNCATE TABLE {self.table}")

        timeformat_clause = f"TIMEFORMAT AS '{self.timeformat}'" if self.timeformat else ""

        copy_sql = f"""
            COPY {self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            REGION '{self.region}'
            FORMAT AS JSON '{self.json_path}'
            {timeformat_clause}
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
        """

        self.log.info("Running COPY command for %s", self.table)
        redshift.run(copy_sql)
        self.log.info("StageToRedshiftOperator finished for %s", self.table)