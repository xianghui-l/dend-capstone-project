from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_csv_template = """
        COPY {table}
        FROM {object_path}
        ACCESS_KEY_ID {access_key_id}
        SECRET_ACCESS_KEY {secret_access_key}
        REGION '{region}'
        CSV DELIMITER '{delimiter}'
        IGNOREHEADER {ignore_header}
        DATEFORMAT '{date_format}'
        TIMEFORMAT '{time_format}'
        ACCEPTINVCHARS;
    """

    @apply_defaults
    def __init__(
        self,
        # define operators params
        redshift_conn_id='',
        aws_credentials_id='',
        table='',
        s3_bucket='',
        s3_key='',
        region='',
        delimiter=',',
        ignore_header=1,
        date_format='YYYY-MM-DD',
        time_format='auto',
        create_sql='',
        truncate=False,
        *args, **kwargs
    ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # map params
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.delimiter = delimiter
        self.ignore_header = ignore_header
        self.date_format = date_format
        self.time_format = time_format
        self.create_sql = create_sql
        self.truncate = truncate


    def execute(self, context):
        """Stage data from S3 to Redshift.
        """
        # create AWS hook
        aws_hook = AwsHook(self.aws_credentials_id)
        # get the credentials from the AWS hook
        credentials = aws_hook.get_credentials()
        # create connection to Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Creating {self.table} table if not exists')

        # create table if not exists
        redshift_hook.run(self.create_sql)

        # truncate the table if the param is set to True
        if self.truncate:
            self.log.info(f'Truncating {self.table} table')
            redshift_hook.run(f'TRUNCATE {self.table};')

        self.log.info(f'Copying {self.table} data from S3 to Redshift')

        # format the copy SQL statement using the params
        copy_sql = self.copy_csv_template.format(
            table=self.table,
            object_path=f"s3://{self.s3_bucket}/{self.s3_key}",
            access_key_id=credentials.access_key,
            secret_access_key=credentials.secret_key,
            region=self.region,
            delimiter=self.delimiter,
            ignore_header=self.ignore_header,
            # json_path=self.json_path,
            date_format=self.date_format,
            time_format=self.time_format
        )

        # run the copy statement
        redshift_hook.run(copy_sql)

        self.log.info(f'Successfully copied {self.table} data from S3 to Redshift')
