from airflow.decorators import task
import logging

logger = logging.getLogger(__name__)


@task
def init_mysql(table, engine):
    logger.info('初始化数据表')
    table.metadata.create_all(engine)