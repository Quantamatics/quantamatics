import pandas as pd
import numpy as np

from quantamatics.core import settings
from quantamatics.core.APIClient import Session
from quantamatics.core.utils import QException
from quantamatics.data.securityMaster import Instrument
from quantamatics.data.fundamentals import KPI
from quantamatics.providers.panels import Panel
from quantamatics.core.settings import DatasetTypes

class FacteusSummaryBase(Panel):
    def __init__(self, panelName):
        super().__init__(panelName=panelName, panelDatasetType = DatasetTypes.ConsumerCardPayments)

        self.mapping = {
            'measures': {
                'Spend': {
                    'pre_process': lambda x: np.sum(x['Spend']),
                    'agg_func': lambda x: np.sum(x['Spend']),
                    'request_field_name': 'spend',
                    'request_field_name_normalized': 'normalized spend',
                    'return_field_name': 'spend'
                },
                'Transaction Count': {
                    'pre_process': lambda x: np.sum(x['Transaction Count']),
                    'agg_func': lambda x: np.sum(x['Transaction Count']),
                    'request_field_name': 'transaction count',
                    'request_field_name_normalized': 'normalized transaction count',
                    'return_field_name': 'transaction_count'
                },
                'Cardholder Count': {
                    'pre_process': lambda x: np.mean(x['Cardholder Count']),
                    'agg_func': lambda x: np.sum(x['Transaction Count']) / np.mean(pd.Series(x['Transactions per Card']).replace(0, np.nan)),
                    'request_field_name': 'card count',
                    'request_field_name_normalized': 'normalized card count',
                    'return_field_name': 'card_count'
                },
                'Spend per Transaction': {
                    'pre_process': lambda x: np.sum(x['Spend']) / np.sum(pd.Series(x['Transaction Count']).replace(0, np.nan)),
                    'agg_func': lambda x: np.sum(x['Spend']) / np.sum(pd.Series(x['Transaction Count']).replace(0, np.nan))
                },
                'Transactions per Card': {
                    'pre_process': lambda x: np.mean(x['Transaction Count'] / pd.Series(x['Cardholder Count']).replace(0, np.nan)),
                    'agg_func': lambda x: np.mean(x['Transactions per Card'])
                },
                'Spend per Card': {
                    'pre_process': lambda x: np.mean(x['Spend'] / pd.Series(x['Cardholder Count']).replace(0, np.nan)),
                    'agg_func': lambda x: np.sum(x['Spend']) / (np.sum(x['Transaction Count']) / np.mean(pd.Series(x['Transactions per Card']).replace(0, np.nan)))
                }
            },
            

            'dimensions': {'Age Group': {'request_field_name': 'generation', 'return_field_name': 'generation'},
                           'Region': {'request_field_name': 'region', 'return_field_name': 'region'},
                           'Card Type': {'request_field_name': 'card type', 'return_field_name': 'card_type'},
                           'Date': {'request_field_name': 'date', 'return_field_name': 'date'},
                           'Ticker': {'request_field_name': 'ticker', 'return_field_name': 'ticker'},
                           'Exchange': {'request_field_name': 'exchange', 'return_field_name': 'exchange'},
                           'Brand': {'request_field_name': 'merchant', 'return_field_name': 'merchant'}
                           }
            ,
            'granularity': 'Daily'
        }

    def loadData(self, ticker: str = None, instrumentObj: Instrument = None, kpiObj: KPI = None,
                 brands: str(list) = None, dimensions: str(list) = ['Date'],
                 measures: str(list) = ['Spend', 'Transaction Count', 'Cardholder Count'], normalizedMeasures: bool = True):

        ticker = self.getTicker(instrumentObj, ticker)

        normalizedSuffix = ''
        if normalizedMeasures:
            normalizedSuffix = '_normalized'

        if measures is None:
            request_measures = [x['request_field_name'+normalizedSuffix] for x in self.mapping['measures'].values() if 'request_field_name'+normalizedSuffix in x]
        else:
            request_measures = [self.mapping['measures'][x]['request_field_name'+normalizedSuffix] for x in measures]

        request_dimensions = [self.mapping['dimensions'][x]['request_field_name'] for x in dimensions]

        merchants = None
        if brands is not None:
            merchants = brands

        if kpiObj is not None:
            kpiID = kpiObj.kpiID

            if len(kpiObj.brands) > 0:
                merchants = kpiObj.brands
        session = Session()
        resultDF =session.apiWrapper(enableCompressionOverride=True,
            api_relative_path = '/api/data/panel/summaryDataLoad',
            params = {
                'panelName': self.panelName,
                'ticker': ticker,
                'merchants': merchants,
                'dimensions': request_dimensions,
                'measures': request_measures
            }
        )

        self.dataDF = resultDF
        self.preProcess(dimensions = dimensions)

        return self.dataDF

class FacteusUSCPSummary(FacteusSummaryBase):
    def __init__(self):
        super().__init__(panelName=settings.SupportedPanels.FacteusUSCPSummaryLatest)

class FacteusPulseBacktest(FacteusSummaryBase):
    def __init__(self):
        super().__init__(panelName=settings.SupportedPanels.FacteusPulseBacktest)

class FacteusPulse(FacteusSummaryBase):
    def __init__(self):
        super().__init__(panelName=settings.SupportedPanels.FacteusPulse)

class FacteusUSCPSummaryDemo(FacteusSummaryBase):
    def __init__(self):
        super().__init__(panelName=settings.SupportedPanels.FacteusUSCPSummaryDemo)
