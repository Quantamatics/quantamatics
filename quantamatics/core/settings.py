from types import SimpleNamespace
import os
import logging

# Symbology
from quantamatics.providers import config


__symbologytypes = {'Bloomberg': 'Bloomberg',
                    'Facteus': 'Facteus',
                    'ISIN': 'ISIN',
                    'CapIQ': 'CapIQ'}

SymbologyTypes = SimpleNamespace(**__symbologytypes)

# Forecast Models
__modellist = {'Linear': 'ModelOLS',
               'Robust': 'ModelRLM',
               'TimeSeries': 'ModelSARIMA',
               'Ensemble': 'Ensemble'}

ForecastModels = SimpleNamespace(**__modellist)

# Financial Statements
__finstatements = {'IncomeStatement': 'Income Statement',
                   'CashFlow': 'Cash Flow',
                   'BalanceSheet': 'Balance Sheet',
                   'Other': 'Other'}

FinancialStatements = SimpleNamespace(**__finstatements)

# Panel Dataset types

__datasettypes = {'ConsumerCardPayments': 'Consumer Card Payments'}

DatasetTypes = SimpleNamespace(**__datasettypes)

# Available Panels

SupportedPanels = SimpleNamespace(**config.__panels)
PanelClasses = config.__panelClasses


# Defaults
DefaultSymbologyType = SymbologyTypes.Facteus
DefaultForecastModel = ForecastModels.Linear

# Retrieve API endpoint when running within Quantamatics Platform
APIEndpoint = os.environ.get('QMC_API_ENDPOINT')

# If environment variable is not set, default to production endpoint
if APIEndpoint is None:
    APIEndpoint = 'https://api.quantamatics.com'

# Logging
__loglevels = {'DEBUG': logging.DEBUG,
               'DISABLED': logging.NOTSET}

LogLevels = SimpleNamespace(**__loglevels)

# Default Logging Mode- Set to LogLevels.DEBUG for additional debug information
LoggingLevel = LogLevels.DISABLED

__params_types = {'URL': 'URL', 
                'JSON': 'JSON'}

ParamsTypes = SimpleNamespace(**__params_types)

__method_types = {'GET': 'GET', 
                'POST': 'POST'}

MethodTypes = SimpleNamespace(**__method_types)
