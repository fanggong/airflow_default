from airflow.decorators import task
from datetime import timedelta
import logging


logger = logging.getLogger(__name__)


def okx_fetch(api, param):
    param = param or {} 
    try:
        logger.info(f'调用接口 {api.__name__}，参数：{param}')
        response = api(**param)
        
        if response['code'] == '0':
            logger.info(f"接口返回数据成功: {response['data']}")
            return response['data']
        else:
            logger.info(f"接口返回数据失败: {response['msg']}")
            return []
    except Exception as e:
        logger.error(f'接口请求失败: {str(e)}')
        raise e
