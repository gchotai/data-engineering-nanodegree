from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    truncate_query = """ Truncate Table {table} """
    insert_query = """ insert into {table} {query} """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 query="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.query=query

    def execute(self, context):
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('table truncated before insert data')
        redshift.run(LoadDimensionOperator.truncate_query.format(table=self.table))
        
        redshift.run(LoadDimensionOperator.insert_query.format(table=self.table, query=self.query))
        self.log.info(f'Data inserted into {self.table} table.')