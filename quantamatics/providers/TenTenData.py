import pandas as pd
import numpy as np

from quantamatics.core.utils import OrderDataFrameColumns
from quantamatics.core import settings
from quantamatics.core.APIClient import Session
from quantamatics.core.utils import QException
from quantamatics.data.securityMaster import Instrument
from quantamatics.data.fundamentals import KPI
from quantamatics.providers.panels import Panel
from quantamatics.core.settings import ParamsTypes
from quantamatics.core.settings import DatasetTypes

# TODO: Split into TenTenDenominatorBase and TenTenFixedBase
class TenTenBase(Panel):
    def __init__(self, panelName):
        super().__init__(panelName=panelName, panelDatasetType = DatasetTypes.ConsumerCardPayments)

        self.mapping = {
            'measures': {
                        'Spend': {
                            'pre_process': lambda x: np.sum(x['sales_index_numerator']) / np.sum(x['sales_index_denominator']),
                            'agg_func': lambda x: np.sum(x['Spend Index Numerator']) / np.sum(x['Spend Index Denominator'])
                        },
                        'Transaction Count': {
                            'pre_process': lambda x: np.sum(x['num_trans_index']),
                            'agg_func': lambda x: (np.sum(x['Transaction Count Index Numerator Raw']) * 10000000.0) / np.sum(x['Spend Index Denominator Raw'])
                        },
                        'Cardholder Count': {
                            'pre_process': lambda x: np.sum(x['num_cust_index']),
                            'agg_func': lambda x: np.mean(x['Cardholder Count']) #(np.mean(x['num_cust_index_numerator_raw']) * 10000000) / np.sum(x['sales_index_denominator_raw'])
                         },                            
                         'Spend per Transaction': {
                             'pre_process': lambda x: np.sum(x['avg_dollar_per_trans']),
                             'agg_func': lambda x: np.sum(x['Spend Index Numerator Raw']) / np.sum(x['Transaction Count Index Numerator Raw'])
                         },
                         'Transactions per Card': {
                             'pre_process': lambda x: np.sum(x['num_trans_index']) / np.sum(x['num_cust_index']),
                             'agg_func': lambda x: np.mean(x['Transaction Count'] / x['Cardholder Count'])
                         },
                         'Spend per Card': {
                             'pre_process': lambda x: np.sum(x['avg_dollar_per_cust']),
                             'agg_func': lambda x: np.mean(x['Spend per Card']) #lambda x: np.sum(x['Spend Index Numerator Raw']) / np.mean(x['num_cust_index_numerator_raw'])
                         }, 
                        'Spend Index Numerator': {
                            'pre_process': lambda x: np.sum(x['sales_index_numerator']),
                            'agg_func': lambda x: np.sum(x['Spend Index Numerator'])
                        },
                        'Spend Index Numerator Raw': {
                            'pre_process': lambda x: np.sum(x['sales_index_numerator']) * 32696.0,
                            'agg_func': lambda x: np.sum(x['Spend Index Numerator Raw'])
                        },
                        'Spend Index Denominator': {
                            'pre_process': lambda x: np.sum(x['sales_index_denominator']),
                            'agg_func': lambda x: np.sum(x['Spend Index Denominator'])
                        },
                        'Spend Index Denominator Raw': {
                          'pre_process': lambda x: (np.sum(x['sales_index_denominator']) * 114514605.0),
                          'agg_func': lambda x: np.sum(x['Spend Index Denominator Raw'])
                        },
                        'Transaction Count Index': {
                            'pre_process': lambda x: (((np.sum(x['num_trans_index']) * (np.sum(x['sales_index_denominator']) * 114514605.0)) / 10000000.0) * 10000000.0) / (np.sum(x['sales_index_denominator']) * 114514605.0),
                            'agg_func': lambda x: (((np.sum(x['Transaction Count']) * (np.sum(x['Spend Index Denominator']) * 114514605.0)) / 10000000.0) * 10000000.0) / (np.sum(x['Spend Index Denominator']) * 114514605.0)
#                             'agg_func': lambda x: np.sum(x['Transaction Count Index'])
                        },
                        'Transaction Count Index Numerator Raw': {
                            'pre_process': lambda x: ((np.sum(x['num_trans_index']) * (np.sum(x['sales_index_denominator']) * 114514605.0)) / 10000000.0), 
                            'agg_func': lambda x: ((np.sum(x['Transaction Count Index']) * (np.sum(x['Spend Index Denominator']) * 114514605.0)) / 10000000.0)
                        },
                        'Cardholder Count Index Numerator Raw': {
                            'pre_process': lambda x: ((np.sum(x['num_cust_index']) * (np.sum(x['sales_index_denominator']) * 114514605.0)) / 10000000.0), 
                            'agg_func': lambda x: ((np.sum(x['Cardholder Count']) * (np.sum(x['Spend Index Denominator']) * 114514605.0)) / 10000000.0)
                        }

                     }
            ,

            'dimensions': {'Date': {'request_field_name': 'reportdate', 'return_field_name': 'reportdate'},
                           'Ticker': {'request_field_name': 'ticker', 'return_field_name': 'ticker'}
                           }
            ,
            'granularity': 'Daily'
        }

    def loadData(self, ticker: str = None, instrumentObj: Instrument = None, kpiObj: KPI = None,
                 brands: str(list) = None, dimensions: str(list) = ['Date'],
                 measures: str(list) = ['Spend', 'Transaction Count', 'Cardholder Count'],
                 normalizedMeasures: bool = True):

        ticker = self.getTicker(instrumentObj, ticker)

        merchants = None
        if brands is not None:
            merchants = brands

        if kpiObj is not None:
            kpiID = kpiObj.kpiID

            if len(kpiObj.brands) > 0:
                merchants = kpiObj.brands
        session = Session()

        fixed_indicator = ''
        if ('fixed' in self.panelName.lower()):
            fixed_indicator = '_fp'
        
        resultDF = None
        if ('combined' in self.panelName.lower()):            
            for panel in ['panel1', 'panel2']:
                table_base = f'pub.consumer_data.card_us_v201803.portal.{panel}.reports.combined.sales_tracker{fixed_indicator}.fiscal.daily'

                curPanelresultDF = session.apiWrapper(enableCompressionOverride=True,
                    api_relative_path = '/api/data/TenTen/getDataByTicker',
                    params=  {
                                "tableName": table_base,
                                "ticker": ticker
                             }, 
                    params_type=ParamsTypes.JSON
                )
                
                if resultDF is None:
                    resultDF = curPanelresultDF
                else:
                    resultDF = resultDF.set_index('reportdate') + curPanelresultDF.set_index('reportdate')
                    resultDF = resultDF.reset_index()
        else:
            if ('credit' in self.panelName.lower()):
                panel = 'panel1'
            elif ('debit' in self.panelName.lower()):
                panel = 'panel2'

            table_base = f'pub.consumer_data.card_us_v201803.portal.{panel}.reports.combined.sales_tracker{fixed_indicator}.fiscal.daily'

            resultDF = session.apiWrapper(enableCompressionOverride=True,
                api_relative_path = '/api/data/TenTen/getDataByTicker',
                params=  {
                            "tableName": table_base,
                            "ticker": ticker
                         }, 
                params_type=ParamsTypes.JSON
            )

        resultDF['reportdate'] = pd.to_datetime(resultDF['reportdate'], format='%Y%m%d')

        self.dataDF = resultDF
        self.mapReturnFields()
        # Ensure we have a row for every day in the period (e.g. fixes for ROST during pandemic closures)
        if 'Date' in dimensions:
            self.completeDailyRange()
            self.dataDF['Date'] = self.dataDF['Date'].apply(lambda x: pd.to_datetime(x))

        self.dataDF = self.applyMeasures(dimensions=dimensions, functionName = 'pre_process')
        self.dataDF = self.applyMeasures(dimensions=dimensions, functionName = 'agg_func')
#         self.dataDF = self.dataDF.drop(['Spend Index Numerator', 'Spend Index Numerator Raw', 'Spend Index Denominator'], axis=1) #'num_trans_index_numerator_raw', 'num_cust_index_numerator_raw', 'sales_index_denominator_raw', 'num_trans_index', 
#         self.dataDF = OrderDataFrameColumns(df = self.dataDF, column_order=['Date', 'Spend', 'Transaction Count', 'Cardholder Count', 'Spend per Transaction', 'Transactions per Card', 'Spend per Card'])
        
        return self.dataDF

class TenTenFixedBase(Panel):
    def __init__(self, panelName):
        super().__init__(panelName=panelName, panelDatasetType = DatasetTypes.ConsumerCardPayments)

        self.mapping = {
            'measures': {
                        'Spend': {
                            'pre_process': lambda x: np.sum(x['sales_index']),
                            'agg_func': lambda x: np.sum(x['Spend'])
                        },
                        'Transaction Count': {
                            'pre_process': lambda x: np.sum(x['num_trans_index']),
                            'agg_func': lambda x: (np.sum(x['Transaction Count']))
                        },
                        'Cardholder Count': {
                            'pre_process': lambda x: np.sum(x['num_cust_index']),
                            'agg_func': lambda x: np.mean(x['Cardholder Count']) #(np.mean(x['num_cust_index_numerator_raw']) * 10000000) / np.sum(x['sales_index_denominator_raw'])
                         },                            
                         'Spend per Transaction': {
                             'pre_process': lambda x: np.sum(x['avg_dollar_per_trans']),
                             'agg_func': lambda x: np.sum(x['Spend']) / np.sum(x['Transaction Count'])
                         },
                         'Transactions per Card': {
                             'pre_process': lambda x: np.sum(x['num_trans_index']) / np.sum(x['num_cust_index']),
                             'agg_func': lambda x: np.mean(x['Transaction Count'] / x['Cardholder Count'])
                         },
                         'Spend per Card': {
                             'pre_process': lambda x: np.sum(x['avg_dollar_per_cust']),
                             'agg_func': lambda x: np.mean(x['Spend per Card']) #lambda x: np.sum(x['Spend Index Numerator Raw']) / np.mean(x['num_cust_index_numerator_raw'])
                         }, 
                     }
            ,

            'dimensions': {'Date': {'request_field_name': 'reportdate', 'return_field_name': 'reportdate'},
                           'Ticker': {'request_field_name': 'ticker', 'return_field_name': 'ticker'}
                           }
            ,
            'granularity': 'Daily'
        }

    def loadData(self, ticker: str = None, instrumentObj: Instrument = None, kpiObj: KPI = None,
                 brands: str(list) = None, dimensions: str(list) = ['Date'],
                 measures: str(list) = ['Spend', 'Transaction Count', 'Cardholder Count'],
                 normalizedMeasures: bool = True):

        ticker = self.getTicker(instrumentObj, ticker)

        merchants = None
        if brands is not None:
            merchants = brands

        if kpiObj is not None:
            kpiID = kpiObj.kpiID

            if len(kpiObj.brands) > 0:
                merchants = kpiObj.brands
        session = Session()

        fixed_indicator = ''
        if ('fixed' in self.panelName.lower()):
            fixed_indicator = '_fp'
        
        resultDF = None
        if ('combined' in self.panelName.lower()):            
            for panel in ['panel1', 'panel2']:
                table_base = f'pub.consumer_data.card_us_v201803.portal.{panel}.reports.combined.sales_tracker{fixed_indicator}.fiscal.daily'

                curPanelresultDF = session.apiWrapper(enableCompressionOverride=True,
                    api_relative_path = '/api/data/TenTen/getDataByTicker',
                    params=  {
                                "tableName": table_base,
                                "ticker": ticker
                             }, 
                    params_type=ParamsTypes.JSON
                )
                
                if resultDF is None:
                    resultDF = curPanelresultDF
                else:
                    resultDF = resultDF.set_index('reportdate') + curPanelresultDF.set_index('reportdate')
                    resultDF = resultDF.reset_index()
        else:
            if ('credit' in self.panelName.lower()):
                panel = 'panel1'
            elif ('debit' in self.panelName.lower()):
                panel = 'panel2'

            table_base = f'pub.consumer_data.card_us_v201803.portal.{panel}.reports.combined.sales_tracker{fixed_indicator}.fiscal.daily'

            resultDF = session.apiWrapper(enableCompressionOverride=True,
                api_relative_path = '/api/data/TenTen/getDataByTicker',
                params=  {
                            "tableName": table_base,
                            "ticker": ticker
                         }, 
                params_type=ParamsTypes.JSON
            )

        resultDF['reportdate'] = pd.to_datetime(resultDF['reportdate'], format='%Y%m%d')

        self.dataDF = resultDF
        self.mapReturnFields()
        # Ensure we have a row for every day in the period (e.g. fixes for ROST during pandemic closures)
        if 'Date' in dimensions:
            self.completeDailyRange()
            self.dataDF['Date'] = self.dataDF['Date'].apply(lambda x: pd.to_datetime(x))

        self.dataDF = self.applyMeasures(dimensions=dimensions, functionName = 'pre_process')
        self.dataDF = self.applyMeasures(dimensions=dimensions, functionName = 'agg_func')
        
        return self.dataDF

class TenTenCreditFixedPanel(TenTenFixedBase):
    def __init__(self):
        super().__init__(panelName=settings.SupportedPanels.TenTenCreditFixedPanel)

class TenTenDebitFixedPanel(TenTenFixedBase):
    def __init__(self):
        super().__init__(panelName=settings.SupportedPanels.TenTenDebitFixedPanel)

class TenTenCombinedFixedPanel(TenTenFixedBase):
    def __init__(self):
        super().__init__(panelName=settings.SupportedPanels.TenTenCombinedFixedPanel)

class TenTenCreditDenominatorPanel(TenTenBase):
    def __init__(self):
        super().__init__(panelName=settings.SupportedPanels.TenTenCreditDenominatorPanel)

class TenTenDebitDenominatorPanel(TenTenBase):
    def __init__(self):
        super().__init__(panelName=settings.SupportedPanels.TenTenDebitDenominatorPanel)

class TenTenCombinedDenominatorPanel(TenTenBase):
    def __init__(self):
        super().__init__(panelName=settings.SupportedPanels.TenTenCombinedDenominatorPanel)

