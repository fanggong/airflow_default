from airflow.models.baseoperator import BaseOperator

class OkxFetchOperator(BaseOperator):
    """OKX 数据获取 Operator

    :param api: 已封装的 API 方法（可调用对象）
    :param params: 传入 API 的参数
    """

    def __init__(self, api, params=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api = api
        self.params = params or {}

    def execute(self, context):
        """执行 API 请求"""
        try:
            self.log.info(f'调用接口 {self.api.__name__}，参数：{self.params}')
            response = self.api(**self.params)
            
            if response['code'] == '0':
                self.log.info(f'接口返回数据成功: {response['data']}')
                return response
            else:
                self.log.info(f'接口返回数据失败: {response['msg']}')
        except Exception as e:
            self.log.error(f'接口请求失败: {str(e)}')
            raise e
