from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="redshift",
        tests=None,
        *args,
        **kwargs
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests or []

    def execute(self, context):
        if not self.tests:
            raise ValueError("DataQualityOperator: No tests provided")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for i, test in enumerate(self.tests, start=1):
            check_sql = test.get("check_sql")
            expected = test.get("expected_result")
            comparison = test.get("comparison", "==")  # default equality

            if not check_sql:
                raise ValueError(f"DataQualityOperator: test #{i} missing 'check_sql'")

            records = redshift.get_records(check_sql)
            if not records or not records[0] or records[0][0] is None:
                raise ValueError(f"DataQualityOperator: test #{i} returned no result: {check_sql}")

            actual = records[0][0]

            passed = False
            if comparison == "==":
                passed = (actual == expected)
            elif comparison == "!=":
                passed = (actual != expected)
            elif comparison == ">":
                passed = (actual > expected)
            elif comparison == ">=":
                passed = (actual >= expected)
            elif comparison == "<":
                passed = (actual < expected)
            elif comparison == "<=":
                passed = (actual <= expected)
            else:
                raise ValueError(f"DataQualityOperator: invalid comparison '{comparison}' in test #{i}")

            if not passed:
                raise ValueError(
                    f"DataQualityOperator FAILED test #{i}. "
                    f"SQL: {check_sql} | actual: {actual} {comparison} expected: {expected} (FAILED)"
                )

            self.log.info(
                "DataQualityOperator PASSED test #%s: actual=%s %s expected=%s",
                i, actual, comparison, expected
            )

        self.log.info("DataQualityOperator: all %s tests passed", len(self.tests))
