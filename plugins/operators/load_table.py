from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadTableOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(
        self,
        # define operators params
        redshift_conn_id='',
        create_sql='',
        table='',
        select_sql='',
        truncate=False,
        *args, **kwargs):

        super(LoadTableOperator, self).__init__(*args, **kwargs)
        # map params
        self.redshift_conn_id = redshift_conn_id
        self.create_sql = create_sql
        self.table = table
        self.select_sql = select_sql
        self.truncate = truncate

    def execute(self, context):
        """Insert data into specified fact table.
        """
        # create connection to Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Creating {self.table} table if not exists')

        # create table if not exists
        redshift_hook.run(self.create_sql)

        # truncate the table if the param is set to True
        if self.truncate:
            self.log.info(f'Truncating {self.table} table')
            redshift_hook.run(f'TRUNCATE {self.table};')

        self.log.info(f'Loading {self.table} table')

        # format insert statement
        insert_sql = f'INSERT INTO {self.table} ({self.select_sql});'

        # insert data into fact table
        redshift_hook.run(insert_sql)

        self.log.info(f'Successfully loaded {self.table} table')
