from airflow.decorators import task
from sqlalchemy.dialects.mysql import insert
import logging

logger = logging.getLogger(__name__)

@task
def sync_mysql(data_list, table, session, type='full'):
    if not data_list:
        logger.info('数据为空，跳过同步')
        return

    try:
        if type == 'full':
            logger.info(f"执行全量更新，清空表 {table.__tablename__}")
            with session.begin_nested():
                session.query(table).delete()
                for data in data_list:
                    new_record = table(**data)
                    session.add(new_record)
            session.commit()

        elif type == 'increment':
            logger.info(f"插入/更新 {len(data_list)} 条数据到 {table.__tablename__}")
            for data in data_list:
                insert_stmt = insert(table).values(**data)
                update_stmt = {key: insert_stmt.inserted[key] for key in data}
                session.execute(insert_stmt.on_duplicate_key_update(**update_stmt))
            session.commit()
            
        logger.info(f"成功同步 {len(data_list)} 条数据到 {table.__tablename__}")

    except Exception as e:
        session.rollback()
        logger.error(f'数据同步失败: {str(e)}')
        raise e
    finally:
        session.close()