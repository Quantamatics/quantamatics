from quantamatics.core import settings
import logging
import pandas as pd


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class QException(Exception):
    def __init__(self, message):
        log = QLog()
        log.logException(message)
        super().__init__(message)


class QLog(metaclass=Singleton):
    def __init__(self):
        return

    def logDebug(self, message):
        if settings.LoggingLevel == settings.LogLevels.DEBUG:
            print(message)

    def logException(self, message):
        if settings.LoggingLevel == settings.LogLevels.DEBUG:
            print(message)


def OrderDataFrameColumns(column_order: str(list) = None, df: pd.DataFrame = None):
    if column_order is None or df is None:
        return 'Column Order or DataFrame empty'
    
    columns = df.columns
    columns = [x for x in columns if x not in column_order]
    columns = column_order + columns
    return df[columns]
