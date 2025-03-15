from airflow.decorators import task
from datetime import timedelta
import logging


logger = logging.getLogger('airflow.task')

@task
def okx_fetch(api, params=None):
    params = params or {} 
    try:
        logger.info(f'调用接口 {api.__name__}，参数：{params}')
        response = api(**params)
        
        if response['code'] == '0':
            logger.info(f"接口返回数据成功: {response['data']}")
            return response
        else:
            logger.info(f"接口返回数据失败: {response['msg']}")
    except Exception as e:
        logger.error(f'接口请求失败: {str(e)}')
        raise e
