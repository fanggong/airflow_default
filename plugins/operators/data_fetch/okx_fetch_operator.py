from airflow.models.baseoperator import BaseOperator


class OkxFetchOperator(BaseOperator):
    def __init__(self, api, param=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api = api
        self.param = param or {}

    def execute(self, context):
        """执行 API 请求"""
        try:
            self.log.info(f'调用接口 {self.api.__name__}，参数：{self.param}')
            response = self.api(**self.param)
            
            if response['code'] == '0':
                self.log.info(f"接口返回数据成功: {response['data']}")
                return response['data']
            else:
                self.log.info(f"接口返回数据失败: {response['msg']}")
        except Exception as e:
            self.log.error(f'接口请求失败: {str(e)}')
            raise e
