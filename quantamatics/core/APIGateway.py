import json
import pandas as pd

from quantamatics.core.APIClient import Session
from quantamatics.core.utils import QException, QLog, Singleton
from quantamatics.core.settings import MethodTypes, ParamsTypes

class APIGatewayClient(metaclass=Singleton):
    def __init__(self):
        self.session = Session()
        self.logger = QLog()
        self.apiDirectory = None
        return

    def getAPIList(self):

        _, response_text = self.session.handleRequest(
            api_relative_path='/api/function/getAll'
        )

        apiDirectory = json.loads(response_text)

        self.apiDirectory = apiDirectory

        apiList = pd.DataFrame(columns = ['Name', 'Provider', 'Description'])
        for apiMetaData in self.apiDirectory:
            apiList = pd.concat(
                [
                    apiList, 
                    pd.DataFrame(
                        [
                            [
                                apiMetaData['name'],
                                apiMetaData['assetName'],
                                apiMetaData['description']
                            ]
                        ]
                    )
                ]
            )

        return apiList

    def getAPIMetaData(self, gatewayAPIName: str) -> dict:
        if self.apiDirectory is None:
            self.getAPIList()

        apiMetaData=list(filter(lambda x:x["name"]==gatewayAPIName,self.apiDirectory))
        #TODO: remove id,type,functionId & assetId
        return apiMetaData

    def restartAPIGatewayEnv(self):
        try:
            self.session.handleRequest(
                api_relative_path='/api/function/restartEnvironment', 
                method_type=MethodTypes.POST
            )
            return True
        except QException:
            return False

    def executeAPICall(self, gatewayAPIName: str,params: dict = {}) -> pd.DataFrame:

        requestParams = {'FunctionName':gatewayAPIName,
                          'BatchId':None,
                          'Args':params }

        _, response_text = self.session.handleRequest(
            api_relative_path='/api/function/runFunction',
            params=requestParams,
            params_type=ParamsTypes.JSON,
            method_type=MethodTypes.POST
        )

        responseData = json.loads(response_text)

        if responseData['error'] is not None:
            self.logger.logDebug(responseData['error'])
            raise QException('Execution Error: %s' % responseData['error'])

        try:
            gatewayResultsDF = pd.read_json(responseData['body'], orient='rows')
        except:
            gatewayResultsDF = pd.DataFrame([[responseData['body']]])
            gatewayResultsDF.columns = ['Result']

        return gatewayResultsDF






