from airflow.models.baseoperator import BaseOperator


class InitMysqlOperator(BaseOperator):
    def __init__(self, table, engine, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table = table
        self.engine = engine

    def execute(self, context):
        self.log.info('初始化数据表')
        self.table.metadata.create_all(self.engine)