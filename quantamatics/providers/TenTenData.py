import pandas as pd
import numpy as np

from quantamatics.core import settings
from quantamatics.core.APIClient import Session
from quantamatics.core.utils import QException
from quantamatics.data.securityMaster import Instrument
from quantamatics.data.fundamentals import KPI
from quantamatics.providers.panels import Panel

class TenTenBase(Panel):
    def __init__(self, panelName):
        super().__init__(panelName=panelName)

        self.mapping = {
            'measures': {'Spend': {'agg_func': lambda x: np.sum(x['Spend']), 'request_field_name': 'spend',
                                   'request_field_name_normalized': 'normalized spend', 'return_field_name': 'spend'},
                         'Transaction Count': {'agg_func': lambda x: np.sum(x['Transaction Count']),
                                               'request_field_name': 'transaction count',
                                               'request_field_name_normalized': 'normalized transaction count',
                                               'return_field_name': 'transaction_count'},
                         'Cardholder Count': {'agg_func': lambda x: np.mean(x['Cardholder Count']),
                                              'request_field_name': 'card count',
                                              'request_field_name_normalized': 'normalized card count',
                                              'return_field_name': 'card_count'},
                         'Spend per Transaction': {
                             'agg_func': lambda x: np.sum(x['Spend']) / np.sum(x['Transaction Count'])},
                         'Transactions per Card': {
                             'agg_func': lambda x: np.mean(x['Transaction Count'] / x['Cardholder Count'])},
                         'Spend per Card': {'agg_func': lambda x: np.mean(x['Spend'] / x['Cardholder Count'])}
                         }
            ,

            'dimensions': {'Date': {'request_field_name': 'date', 'return_field_name': 'date'},
                           'Ticker': {'request_field_name': 'ticker', 'return_field_name': 'ticker'},
                           'Exchange': {'request_field_name': 'exchange', 'return_field_name': 'exchange'}
                           }
            ,
            'granularity': 'Daily'
        }

    def loadData(self, ticker: str = None, instrumentObj: Instrument = None, kpiObj: KPI = None,
                 brands: str(list) = None, dimensions: str(list) = ['Date'],
                 measures: str(list) = ['Spend', 'Transaction Count', 'Cardholder Count'],
                 normalizedMeasures: bool = True):

        ticker = self.getTicker(instrumentObj, ticker)

        normalizedMeasures = True
        normalizedSuffix = ''
        if normalizedMeasures:
            normalizedSuffix = '_normalized'

        if measures is None:
            request_measures = [x['request_field_name' + normalizedSuffix] for x in self.mapping['measures'].values() if
                                'request_field_name' + normalizedSuffix in x]
        else:
            request_measures = [self.mapping['measures'][x]['request_field_name' + normalizedSuffix] for x in measures]

        request_dimensions = [self.mapping['dimensions'][x]['request_field_name'] for x in dimensions]

        merchants = None
        if brands is not None:
            merchants = brands

        if kpiObj is not None:
            kpiID = kpiObj.kpiID

            if len(kpiObj.brands) > 0:
                merchants = kpiObj.brands
        session = Session()
        resultDF = session.apiWrapper(enableCompressionOverride=True,
            api_relative_path = '/api/data/panel/summaryDataLoad',
            params=  {
                        'panelName': self.panelName,
                        'ticker': ticker,
                        'dimensions': request_dimensions,
                        'measures': request_measures
                     }
        )

        self.dataDF = resultDF
        self.mapReturnFields()
        # Ensure we have a row for every day in the period (e.g. fixes for ROST during pandemic closures)
        if 'Date' in dimensions:
            self.completeDailyRange()
            self.dataDF['Date'] = self.dataDF['Date'].apply(lambda x: pd.to_datetime(x))

        self.dataDF = self.applyMeasures(dimensions=dimensions)
        return self.dataDF

class TenTenCreditFixedPanel(TenTenBase):
    def __init__(self):
        super().__init__(panelName=settings.SupportedPanels.TenTenCreditFixedPanel)

class TenTenDebitFixedPanel(TenTenBase):
    def __init__(self):
        super().__init__(panelName=settings.SupportedPanels.TenTenDebitFixedPanel)

class TenTenCombinedFixedPanel(TenTenBase):
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

