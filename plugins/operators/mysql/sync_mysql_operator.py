from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.mysql import insert

class SyncMysqlOperator(BaseOperator):
    def __init__(self, table, session, type='full', src='xcom', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.src = src
        self.table = table
        self.session = session
        self.type = type

    def execute(self, context):
        if self.src == 'xcom':
            data_list = context['task_instance'].xcom_pull(task_ids='process', key='return_value')
        
        if not data_list:
            self.log.info('数据为空，跳过同步')
            return

        try:
            if self.type == 'full':
                self.log.info(f"执行全量更新，清空表 {self.table.__tablename__}")
                with self.session.begin_nested():
                    self.session.query(self.table).delete()
                    for data in data_list:
                        new_record = self.table(**data)
                        self.session.add(new_record)
                self.session.commit()

            elif self.type == 'increment':
                self.log.info(f"插入/更新 {len(data_list)} 条数据到 {self.table.__tablename__}")
                for data in data_list:
                    insert_stmt = insert(self.table).values(**data)
                    update_stmt = {key: insert_stmt.inserted[key] for key in data}
                    self.session.execute(insert_stmt.on_duplicate_key_update(**update_stmt))
                self.session.commit()
                
            self.log.info(f"成功同步 {len(data_list)} 条数据到 {self.table.__tablename__}")

        except Exception as e:
            self.session.rollback()
            self.log.error(f'数据同步失败: {str(e)}')
            raise e
        finally:
            self.session.close()
