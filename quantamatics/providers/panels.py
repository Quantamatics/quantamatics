import pandas as pd
import numpy as np
from datetime import timedelta

from quantamatics.core.APIClient import Session
from quantamatics.core.utils import QException, QLog
from quantamatics.core.utils import OrderDataFrameColumns
from quantamatics.core import settings
from quantamatics.data.fundamentals import KPI, CalendarPeriods
from quantamatics.data.securityMaster import Instrument
from quantamatics.core.settings import DatasetTypes


class Panel:
    def __init__(self, panelName: str, panelDatasetType: str):
        self.panelName = panelName
        self.panelDatasetType = panelDatasetType
        self.dataDF = None
        self.logger = QLog()

        session = Session()
        resultDF = session.apiWrapper(
            '/api/data/panel/init',
            {
                'panelName': self.panelName
            }
        )

        if len(resultDF) == 1:
            self.panelID = resultDF['dataset_id'].iloc[0]
            self.providerID = resultDF['provider_id'].iloc[0]
            self.providerName = resultDF['provider_name'].iloc[0]
        elif len(resultDF) == 0:
            self.panelID = None
            self.providerID = None
            self.providerName = None
        elif len(resultDF) > 1:
            raise QException('More than one panel found')

        self.mapping = {
            'measures': {},
            'dimensions': {},
            'granularity': 'Daily'
        }

    def preProcess(self, dimensions: str(list) = None, mapReturnFields: bool = True, preProcessMeasures: bool = True, completeDailyRange: bool = True):
        if mapReturnFields:
            self.mapReturnFields()
        
        if completeDailyRange and 'Date' in dimensions:
            self.completeDailyRange()
            
        if preProcessMeasures:
            self.applyMeasures(dimensions = dimensions, functionName = 'pre_process')

    def getTicker(self,instrumentObj, ticker):
        if instrumentObj is None and ticker is None:
            raise QException('No ticker or instrument provided')
        elif ticker is not None:
            ticker = ticker.split('-')[0]
            ticker = ticker.split(' ')[0]
        elif instrumentObj is not None:
            instrumentSymbol = instrumentObj.getSymbology()['Facteus']
            ticker = instrumentSymbol.split('-')[0]
        return ticker


    def mapReturnFields(self):
        if self.dataDF is None:
            raise QException('No panel data available')

        measureFields = dict([[self.mapping['measures'][x]['return_field_name'], x] for x in self.mapping['measures'] if 'return_field_name' in self.mapping['measures'][x]])
        dimensionFields = dict([[self.mapping['dimensions'][x]['return_field_name'], x] for x in self.mapping['dimensions'] if 'return_field_name' in self.mapping['dimensions'][x]])
        allFields = {**measureFields, **dimensionFields}

        allFieldsToMap = dict([[x, allFields[x]] for x in self.dataDF.columns if x in allFields])

        _dataDF = self.dataDF.rename(allFieldsToMap, axis=1)

        self.dataDF = _dataDF

    def loadData(self, ticker: str = None, instrumentObj: Instrument = None, kpiObj: KPI = None,
                 brands: str(list) = None, dimensions: str(list) = [],
                 measures: str(list) = [],
                 normalizedMeasures: bool = True):

            return None



    def completeDailyRange(self):
        if self.dataDF is None:
            raise QException('No panel data available')

        if 'Date' not in self.dataDF.columns:
            raise QException('Panel data does not have Date column')

        full_date_range = pd.DataFrame(
            pd.date_range(self.dataDF['Date'].min(), self.dataDF['Date'].max()),
            columns=['Date'])

        _dataDF = self.dataDF.set_index('Date').join(
            full_date_range.set_index('Date'), how='outer', rsuffix='_R').reset_index()
        _dataDF['Date'] = _dataDF.apply(
            lambda x: x['Date_R'] if pd.isnull(x['Date']) else x['Date'], axis=1)
        _dataDF = _dataDF.fillna(0)
        self.dataDF = _dataDF
        

    def applyMeasures(self, dataDF: pd.DataFrame = None, dimensions: str(list) = None, functionName: str = 'agg_func', applyAsAggregate: bool = False, inplace: bool = True):
        if dataDF is None and self.dataDF is not None:
            dataDF = self.dataDF
        elif dataDF is None:
            raise QException('No panel data available')

        if self.mapping is None or self.mapping['measures'] is None:
            raise QException('No panel mapping found')

        if dimensions is not None and len(dimensions) > 0:
            _resultDF = pd.DataFrame(dataDF[dimensions])
        else:
            _resultDF = pd.DataFrame()

        measures = self.mapping['measures']
        
        for curMeasure, curMeasureItem in measures.items():
            try:
                if applyAsAggregate:
                    measureDF = pd.DataFrame([curMeasureItem[functionName](dataDF)], columns=[curMeasure])
                else:
                    if type(dataDF) == type(pd.DataFrame()):
                        measureDF = pd.DataFrame(dataDF.apply(curMeasureItem[functionName], axis=1), columns=[curMeasure])
                    else:
                        measureDF = pd.DataFrame(dataDF.apply(curMeasureItem[functionName]), columns=[curMeasure])
                
                _resultDF = pd.concat([_resultDF, measureDF], axis=1)
            except:
                self.logger.logDebug('failed to process %s' % curMeasure )
                continue

        if inplace:
            self.dataDF = _resultDF
        return _resultDF

    def aggregateDataToCalendarPeriods(self, dimensions: str(list) = None, kpiObj: KPI = None,
                                           calendarPeriodsObj: CalendarPeriods = None,
                                           completeCurrentQuarter: bool = True):
        if self.mapping is None:
            raise QException('No panel mapping found')

        if kpiObj is not None:
            measures = {kpiObj.measureName: self.mapping['measures'][kpiObj.measureName]['agg_func']}
        else:
            measures = self.mapping['measures']

        if dimensions is None:
            dimensions = []

        if self.dataDF is None:
            raise QException('No panel data available to aggregate')

        if kpiObj is None and calendarPeriodsObj is None:
            raise QException('Need a KPI or Calendar Periods object to aggregate to')

        # if calendar periods object passed explicitly, override the kpiObj derived calendarperiods. if calendarPeriodsStr is provided then generate our own calendar periods for Weekly, Monthly, etc.
        if calendarPeriodsObj is not None:
            self.calendarPeriods = calendarPeriodsObj
        elif kpiObj is not None:
            self.calendarPeriods = CalendarPeriods(kpiID=kpiObj.kpiID)

        if self.mapping['granularity'] == 'Daily':
            _dataDF = self.dataDF
            _periodsDF = self.calendarPeriods.getPeriods()
            _aggregatedDF = pd.DataFrame()

            _periodsDF['period_start_date']=_periodsDF['period_start_date'].apply(lambda x:pd.to_datetime(x))
            _periodsDF['period_end_date'] = _periodsDF['period_end_date'].apply(lambda x: pd.to_datetime(x))

            # Get number of days in current period
            currentPeriodDayCount = None
            if len(_periodsDF.loc[_periodsDF['is_current_time_period']]) > 0:
                currentPeriodStartDate = _periodsDF.loc[_periodsDF['is_current_time_period']]['period_start_date'].iat[0]
                currentPeriodEndDate = _periodsDF.loc[_periodsDF['is_current_time_period']]['period_end_date'].iat[0]
                maxDate = max(_dataDF['Date'])
                
                if currentPeriodStartDate <= maxDate and currentPeriodEndDate >= maxDate:
                    currentPeriodDayCount = maxDate - currentPeriodStartDate

            for fq in _periodsDF.period_name.unique():
                startDate = _periodsDF[_periodsDF.period_name == fq]['period_start_date'].iat[0]
                endDate = _periodsDF[_periodsDF.period_name == fq]['period_end_date'].iat[0]
                isCurrentQuarter = _periodsDF[_periodsDF.period_name == fq]['is_current_time_period'].iat[0]

                # remove incomplete quarters
                if endDate > max(_dataDF['Date']):
                    if not isCurrentQuarter or (isCurrentQuarter and completeCurrentQuarter):
                        continue
                
                if startDate > max(_dataDF['Date']):
                    continue

                if startDate < min(_dataDF['Date']):
                    continue

                _periodDF = _dataDF[(_dataDF['Date'] >= startDate) & (_dataDF['Date'] <= endDate)]
                endDatePeriodToDate = None
                if currentPeriodDayCount is not None:
                    endDatePeriodToDate = startDate + currentPeriodDayCount
                    _periodToDateDF = _dataDF[(_dataDF['Date'] >= startDate) & (_dataDF['Date'] <= endDatePeriodToDate)]

                _periodAggDF = pd.DataFrame()
                
                
                if len(dimensions) == 0:
                    _periodAggDF = self.applyMeasures(dataDF = _periodDF, applyAsAggregate=True)

                    if currentPeriodDayCount is not None:
                        _periodToDateAggDF = self.applyMeasures(dataDF = _periodToDateDF, applyAsAggregate=True)
                else:
                    _periodAggDFTemp = _periodDF.groupby(dimensions)
                    _periodAggDF = self.applyMeasures(dataDF = _periodAggDFTemp, applyAsAggregate=False, inplace=False)
                    _periodAggDF = _periodAggDF.reset_index()

                    if currentPeriodDayCount is not None:
                        _periodToDateAggDFTemp = _periodToDateDF.groupby(dimensions)
                        _periodToDateAggDF = self.applyMeasures(dataDF = _periodToDateAggDFTemp, applyAsAggregate=False, inplace=False)
                        _periodToDateAggDF = _periodToDateAggDF.reset_index()


                _periodAggDF['PeriodLabel'] = fq
                _periodAggDF['PeriodToDate'] = False
                _periodAggDF['PeriodStartDate'] = startDate
                _periodAggDF['PeriodEndDate'] = endDate
                _periodAggDF['IsCurrentQuarter'] = isCurrentQuarter
                _aggregatedDF = _aggregatedDF.append(_periodAggDF, sort=False, ignore_index=True)
                
                if currentPeriodDayCount is not None:
                    _periodToDateAggDF['PeriodLabel'] = fq
                    _periodToDateAggDF['PeriodToDate'] = True
                    _periodToDateAggDF['PeriodStartDate'] = startDate
                    _periodToDateAggDF['PeriodEndDate'] = endDatePeriodToDate
                    _periodToDateAggDF['IsCurrentQuarter'] = isCurrentQuarter
                    _aggregatedDF = _aggregatedDF.append(_periodToDateAggDF, sort=False, ignore_index=True)

            column_order = ['PeriodLabel', 'PeriodToDate', 'PeriodStartDate', 'PeriodEndDate', 'IsCurrentQuarter']
            _aggregatedDF = OrderDataFrameColumns(column_order, _aggregatedDF)
            _aggregatedDF = _aggregatedDF.sort_values(['PeriodToDate', 'PeriodLabel'])

            self.aggregatedDF = _aggregatedDF

        return self.aggregatedDF

    def getMeasures(self,allMeasures=False):
        measures = []
        for measure in self.mapping['measures']:
            measures.append(measure)

        if not allMeasures:
            if self.dataDF is not None:
                return list(filter(lambda x: x in self.dataDF.columns, measures))
            else:
                return []
        else:
            return measures

    def getDimensions(self,allDimensions=False):
        dimensions = []
        for dim in self.mapping['dimensions']:
            dimensions.append(dim)

        if not allDimensions:
            if self.dataDF is not None:
                return list(filter(lambda x: x in self.dataDF.columns, dimensions))
            else:
                return []
        else:
            return dimensions

    def getGranularity(self):
        return self.mapping['granularity']
