from typing import Tuple
import aiohttp
import asyncio
import nest_asyncio
import json
from json import JSONDecodeError
from jose import jwt
import time
import os
import pandas as pd
import warnings
import pkg_resources
import numpy
from datetime import date

import quantamatics.core.settings as settings
from quantamatics.core.settings import ParamsTypes, MethodTypes
from quantamatics.core.utils import QException, QLog
from quantamatics.core.utils import Singleton

class Session(metaclass=Singleton):
    def __init__(self, enableCaching = True, enableCompression = False):
        # Get Cached API Token when running within Quantamatics Platform
        self._cachedToken = os.environ.get('QMC_API_CACHED_JWT_TOKEN')
        self._apiKey = os.environ.get('QMC_API_KEY')
        self._enableCaching = enableCaching
        self._enableCompression = enableCompression

        try:
            self._version = pkg_resources.require("quantamatics")[0].version
        except:
            self._version = "Unknown"
            
        self._async_session = aiohttp.ClientSession(auto_decompress=True, timeout=aiohttp.ClientTimeout(total=None))
        self.logger = QLog()
        nest_asyncio.apply()

    def __del__(self):
        asyncio.get_event_loop().run_until_complete(self._async_session.close())

    def _getDefaultHeaders(self) -> dict:
        return {
            'User-Agent': 'Quantamatics-PythonLib',
            'X-QMC-Client-Version': self._version
        }

    def getAPIToken(self) -> str:
        if self._cachedToken is not None:
            claims = jwt.get_unverified_claims(self._cachedToken)
            exp_unix_timestamp = claims['exp']
            cur_unix_timestamp = int(time.time())
            if exp_unix_timestamp > (cur_unix_timestamp + 100):
                return self._cachedToken
            else:
                raise QException('Token expired')
        else:
            raise QException('Access Token Not Set. Use login() Method to Set Access Token')

    def login(self, user: str, password: str) -> bool:
        return asyncio.get_event_loop().run_until_complete(self.loginAsync(user, password))        

    async def loginAsync(self, user: str, password: str) -> bool:

        if settings.APIEndpoint is None:
            raise QException('API Endpoint not Defined')

        headers = self._getDefaultHeaders()
        request_data = { 'email': user, 'password': password }
        try:
            async with self._async_session.post(url=settings.APIEndpoint + '/api/account/login', headers=headers, json=request_data) as response:
                status = response.status
                response_text = await response.text()
                if status == 200:
                    response_data = json.loads(response_text)
                    self._cachedToken = response_data['token']
                    return True
                elif status == 401:
                    raise QException('Authentication Failed')
                else:
                    raise QException('Error Accessing EndPoint: %s - %s' % (status, response_text))
        except:
            raise QException('Can not establish connection to %s' % settings.APIEndpoint)

    def setAPIKey(self, apiKey: str):
        self._apiKey = apiKey

    def apiWrapper(self, api_relative_path: str, params: dict = {},
                   enableCompressionOverride = None,
                   enableCachingOverride = None,
                   params_type: str = ParamsTypes.URL,
                   method_type: str = MethodTypes.GET
                   ) -> pd.DataFrame:

        return asyncio.get_event_loop().run_until_complete(
            self.apiWrapperAsync(
                api_relative_path=api_relative_path, 
                params=params,
                enableCompressionOverride=enableCompressionOverride,
                enableCachingOverride=enableCachingOverride,
                params_type=params_type,
                method_type=method_type
            )
        )

    async def apiWrapperAsync(self, api_relative_path: str, 
                                params: dict = {}, enableCompressionOverride = None, 
                                enableCachingOverride = None, 
                                params_type: str = ParamsTypes.URL,
                                method_type: str = MethodTypes.GET) -> pd.DataFrame:
        
        response_headers, response_text = await self.handleRequestAsync(
            api_relative_path=api_relative_path,
            params=params,
            enableCompressionOverride=enableCompressionOverride,
            enableCachingOverride=enableCachingOverride,
            params_type=params_type,
            method_type=method_type
        )

        try:
            result_dict = json.loads(response_text)
        except JSONDecodeError:
            raise QException('Error Encoding JSON: %s ' % response_headers['Content-Encoding'])

        self.logger.logDebug('Response Headers %s' % response_headers)

        try:
            self.logger.logDebug('Result Generation Time (cache): %s' % result_dict['retrievalTime'])
        except:
            pass

        dtypes = {}
        for column_name in result_dict['schema']:
            column_type = result_dict['schema'][column_name]['type']
            column_nullable = result_dict['schema'][column_name]['nullable']

            if column_type == 'datetime.date':
                dtypes[column_name] = 'object'
            elif column_type == 'np.datetime64[ns]':
                dtypes[column_name] = 'object'
            elif column_type == 'int32':
                if column_nullable:
                    dtypes[column_name] ='Int32'
                else:
                    dtypes[column_name] = 'int32'
            elif column_type == 'int64':
                if column_nullable:
                    dtypes[column_name] = 'Int64'
                else:
                    dtypes[column_name] = 'int64'
            elif column_type == 'bool':
                if column_nullable:
                    dtypes[column_name] = 'object'
                else:
                    dtypes[column_name] = 'bool'
            elif (column_type == 'float64' or column_type == 'str' or column_type == 'bytes'):
                dtypes[column_name] = column_type
            else:
                raise QException('Unknown DataFrame Column type returned by API call')

        with warnings.catch_warnings():
            warnings.simplefilter(action='ignore', category=FutureWarning)
            df = pd.read_json(json.dumps(result_dict['data']), orient='columns', dtype=dtypes)

        for column_name in result_dict['schema']:
            column_type = result_dict['schema'][column_name]['type']
            
            # If the number of records is 0, then add the column to the df to ensure we always have the schema
            # at least returned
            if df.shape[0] == 0:
                df[column_name] = []

            if column_type == 'datetime.date':
                df[column_name] = pd.to_datetime(df[column_name]).dt.date
            elif column_type == 'np.datetime64[ns]':
                df[column_name] = pd.to_datetime(df[column_name])

        return df

    def handleRequest(self, api_relative_path: str, 
                                params: dict = {}, enableCompressionOverride = None, 
                                enableCachingOverride = None, 
                                params_type: str = ParamsTypes.URL,
                                method_type: str = MethodTypes.GET) -> Tuple[dict, str]:

        return asyncio.get_event_loop().run_until_complete(
            self.handleRequestAsync(
                api_relative_path=api_relative_path, 
                params=params,
                enableCompressionOverride=enableCompressionOverride,
                enableCachingOverride=enableCachingOverride,
                params_type=params_type,
                method_type=method_type                
            )
        )

    async def handleRequestAsync(self, api_relative_path: str, 
                                params: dict = {}, enableCompressionOverride = None, 
                                enableCachingOverride = None, 
                                params_type: str = ParamsTypes.URL,
                                method_type: str = MethodTypes.GET) -> Tuple[dict, str]:

        self.logger.logDebug(f'=== Call handleRequestAsync to path "{api_relative_path}" ===')
        
        if settings.APIEndpoint is None:
            raise QException('API Endpoint not Defined')

        headers = self._getDefaultHeaders()

        if method_type == MethodTypes.POST:
            headers['Content-Type'] = 'application/json'
            headers['Accept'] = 'application/json'

        if self._cachedToken is not None:
            headers['Authorization'] = 'Bearer ' + self._cachedToken
        elif self._apiKey is not None:
            headers['X-Api-Key'] = self._apiKey
        else:
            raise QException('Not logged in or have not provided an API key')

        if enableCompressionOverride is None:
            enableCompressionOverride = self._enableCompression

        if enableCachingOverride is None:
            enableCachingOverride = self._enableCaching

        if enableCompressionOverride:
            headers['Accept-Encoding'] = 'br'
            self.logger.logDebug('Response Compression Enabled')
        else:
            headers['Accept-Encoding'] = 'identity'
            self.logger.logDebug('Response Compression Disabled')
        if not enableCachingOverride:
            headers['Cache-Control'] = 'no-cache'
            self.logger.logDebug('Caching Disabled')
        else:
            self.logger.logDebug('Caching Enabled')

        if params_type == ParamsTypes.URL:
             # Filter out params where the value is set to None
            params = {k: v for k, v in params.items() if v is not None}

            # Convert params
            for k, v in params.items():
                if type(v) is bool:
                    params[k] = str(v).lower()
                elif type(v) is date:
                    params[k] = str(v)
                elif isinstance(v, numpy.generic):
                    params[k] = v.item()

        start_time = time.time()
        api_full_path = settings.APIEndpoint + api_relative_path

        self.logger.logDebug('Requesting with Paramaters: %s' % params)

        request_args = {
            'headers': headers
        }

        if params_type == ParamsTypes.URL:
            request_args['params'] = params
        elif params_type == ParamsTypes.JSON:
            request_args['json'] = params
        else:
            raise QException('Unexpected params type')

        status = None
        response_text = None
        for i in range(3):

            async with self._async_session.request(method=method_type, url=api_full_path, **request_args) as response:

                status = response.status
                response_text = await response.text()
                self.logger.logDebug("--- %s seconds Round trip time---" % (time.time() - start_time))

                if status >= 500:
                    self.logger.logDebug(f'Received status code {status}, sleeping and retrying...')
                    await asyncio.sleep(10)
                    continue
                elif status == 401:
                    raise QException('Authentication Failed')
                elif status == 400:
                    raise QException('HTTP Client Error: %s - %s' % (status, response_text))
                elif status != 200:
                    raise QException('Error Accessing Endpoint: %s - %s' % (status, response_text))

                self.logger.logDebug('size of response object: %d' % len(response_text))

                return response.headers, response_text

        raise QException('Multiple HTTP Server Errors, Last Error: %s - %s' % (status, response_text))

            

        

