from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        # define operators params
        redshift_conn_id='',
        tables=[],
        *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # map params
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        """Check the number of records in the list of tables.
        """
        self.log.info('DataQualityOperator not implemented yet')

        # create connection to Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # loop through the list of tables
        for table in self.tables:
            # get the record of number of rows in the table
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")

            # raise error if the table returns no record
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")

            # get the number of rows
            num_records = records[0][0]

            logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")
