import pandas as pd
from datetime import date

from quantamatics.core.APIClient import Session
from quantamatics.core.utils import QException


class CalendarPeriods:
    def __init__(self, instrumentID: int = None, kpiID: int = None, useReportedForCurrentQuarter: bool = True):

        if instrumentID is None and kpiID is None:
            raise QException('Instrument or KPI ID must be specified')

        self.instrumentID = instrumentID
        self.kpiID = kpiID

        session = Session()
        self.eventsDF = session.apiWrapper(
            '/api/data/calendarPeriods/init',
            {
                'instrumentId': instrumentID,
                'kpiId': kpiID
            }
        )

        if len(self.eventsDF) == 0:
            raise QException('No calendar periods found for instrument')

        if instrumentID is not None:
            self.eventsDF['instrument_id'] = self.instrumentID

        if kpiID is not None:
            self.eventsDF['kpi_id'] = self.kpiID

        self.eventsDF['is_previous_time_period'] = False
        self.eventsDF['is_current_time_period'] = False
        self.eventsDF['is_next_time_period'] = False

        if useReportedForCurrentQuarter:
            previousTimePeriodDF = self.eventsDF.loc[self.eventsDF['is_historical_time_period'] == True].\
                sort_values(['period_start_date'], ascending=False)['period_name']
            if len(previousTimePeriodDF) > 0:
                previousTimePeriod = previousTimePeriodDF.iloc[0]
                self.eventsDF.loc[self.eventsDF['period_name'] == previousTimePeriod, 'is_previous_time_period'] = True

            currentTimePeriodDF = self.eventsDF.loc[self.eventsDF['is_historical_time_period'] == False].\
                sort_values(['period_start_date'], ascending=True)['period_name']
            if len(currentTimePeriodDF) > 0:
                currentTimePeriod = currentTimePeriodDF.iloc[0]
                self.eventsDF.loc[self.eventsDF['period_name'] == currentTimePeriod, 'is_current_time_period'] = True

            nextTimePeriodDF = self.eventsDF.loc[self.eventsDF['is_historical_time_period'] == False].\
                sort_values(['period_start_date'], ascending=True)['period_name']
            if len(nextTimePeriodDF) > 0:
                nextTimePeriod = nextTimePeriodDF.iloc[1]
                self.eventsDF.loc[self.eventsDF['period_name'] == nextTimePeriod, 'is_next_time_period'] = True
        else:
            self.eventsDF['is_current_time_period'] = self.eventsDF.apply(lambda x: pd.Timestamp.today() >= x['period_start_date'] and pd.Timestamp.today() <= x['period_end_date'], axis=1)

            previousTimePeriodDF = self.eventsDF.loc[pd.Timestamp.today() > self.eventsDF['period_end_date']].sort_values(['period_start_date'], ascending=False)['period_name']
            if len(previousTimePeriodDF) > 0:
                previousTimePeriod = previousTimePeriodDF.iloc[0]
                self.eventsDF.loc[self.eventsDF['period_name'] == previousTimePeriod, 'is_previous_time_period'] = True

            nextTimePeriodDF = self.eventsDF.loc[pd.Timestamp.today() < self.eventsDF['period_start_date']].sort_values(['period_start_date'], ascending=True)['period_name']
            if len(nextTimePeriodDF) > 0:
                nextTimePeriod = nextTimePeriodDF.iloc[0]
                self.eventsDF.loc[self.eventsDF['period_name'] == nextTimePeriod, 'is_next_time_period'] = True

    def getPeriods(self):
        return self.eventsDF

    def getPreviousPeriod(self):
        return self.eventsDF.loc[self.eventsDF['is_previous_time_period']]

    def getCurrentPeriod(self):
        return self.eventsDF.loc[self.eventsDF['is_current_time_period']]

    def getNextPeriod(self):
        return self.eventsDF.loc[self.eventsDF['is_next_time_period']]


class KPI():
    def __init__(self, kpiID: int = None, preLoad: bool = True, instrumentID: int = None, kpiName: str = None,
                 kpiUOM: str = None):
        self.kpiID = kpiID
        self.instrumentID = instrumentID
        self.kpiName = kpiName
        self.kpiUOM = kpiUOM

        if preLoad:
            self.loadKPI()

    def loadKPI(self, kpiID: int = None, instrumentID: int = None, kpiName: str = None,
                kpiUOM: str = None):
        if kpiID is not None:
            self.kpiID = kpiID

        if instrumentID is not None and kpiName is not None and kpiUOM is not None:
            self.instrumentID = instrumentID
            self.kpiName = kpiName
            self.kpiUOM = kpiUOM

        session = Session()
        self.KPIDF = session.apiWrapper(
            '/api/data/kpi/load',
            {
                'kpiId': self.kpiID,
                'instrumentId': self.instrumentID,
                'kpiName': self.kpiName,
                'kpiUom': self.kpiUOM
            }
        )

        self.kpiName = self.KPIDF['kpi_name'].iloc[0]
        self.instrumentID = self.KPIDF['instrument_id'].iloc[0]
        self.kpiUOM = self.KPIDF['unit_of_measure'].iloc[0]
        self.statementType = self.KPIDF['statement_type'].iloc[0]
        self.kpiClass = self.KPIDF['kpi_class'].iloc[0]
        self.kpiID = self.KPIDF['kpi_id'].iloc[0]
        self.measureName = self.KPIDF['measure_name'].iloc[0]

        self.brands = list(self.KPIDF.loc[pd.notna(self.KPIDF['brand_name'])]['brand_name'].drop_duplicates().values)
        # self.panels = list(
        #     self.KPIDF.loc[pd.notna(self.KPIDF['dataset_name'])]['dataset_name'].drop_duplicates().values)

    def getKPIHistory(self, valueType: str = 'Actual', datasetName: str = 'Company KPIs'):

        session=Session()
        self.KPIHistDF = session.apiWrapper(
            '/api/data/kpi/getHistory',
            {
                'kpiId': self.kpiID,
                'valueType': valueType,
                'datasetName': datasetName
            }
        )
        return self.KPIHistDF

    def getLatestEstimate(self, datasetName: str = 'Consensus Estimates', asOfDate: str = str(date.today()),
                          valueType: str = 'Consensus Mean'):

        session=Session()
        self.KPIEstimatesLatestDF = session.apiWrapper(
            '/api/data/kpi/getLatestEstimate',
            {
                'kpiId': self.kpiID,
                'datasetName': datasetName,
                'asOfDate': asOfDate,
                'valueType': valueType
            }
        )

        return self.KPIEstimatesLatestDF

    def getEstimateHistory(self, datasetName: str = 'Consensus Estimates', valueType: str = 'Consensus Mean'):
        session = Session()
        self.KPIEstimatesHistoryDF = session.apiWrapper(
            '/api/data/kpi/getEstimateHistory',
            {
                'kpiId': self.kpiID,
                'datasetName': datasetName,
                'valueType': valueType
            }
        )
        return self.KPIEstimatesHistoryDF


class FinancialStatement():
    def __init__(self, instrumentID: int, panelDatasetType: str = None):
        self.instrumentID = instrumentID
        self.panelDatasetType = panelDatasetType

    def getKPIs(self, primary_only: bool = False, panelDatasetType: str = None):
        if self.panelDatasetType is None and panelDatasetType is not None:
            self.panelDatasetType = panelDatasetType

        session = Session()
        self.KPIDF = session.apiWrapper(
            '/api/data/financialStatement/getKpis',
            {
                'instrumentId': self.instrumentID,
                'primaryOnly': primary_only,
                'datasetType': self.panelDatasetType
            }
        )

        self.kpis = []

        for row in self.KPIDF.iterrows():
            kpi = KPI(kpiID=row[1].kpi_id, instrumentID=row[1].instrument_id, preLoad=False)
            self.kpis.append(kpi)

        return self.kpis

    def getKPIList(self, primary_only: bool = False, panelDatasetType: str = None, financial_statement: str = None):
        if self.panelDatasetType is None and panelDatasetType is not None:
            self.panelDatasetType = panelDatasetType

        session=Session()
        self.KPIDF = session.apiWrapper(
            '/api/data/financialStatement/getKpiList',
            {
                'instrumentId': self.instrumentID,
                'primaryOnly': primary_only,
                'datasetType': self.panelDatasetType
            }
        )

        if financial_statement is not None:
            _KPIDF = self.KPIDF
            _KPIDF = _KPIDF.loc[_KPIDF['statement_type'] == financial_statement]
            self.KPIDF = _KPIDF

        return self.KPIDF








