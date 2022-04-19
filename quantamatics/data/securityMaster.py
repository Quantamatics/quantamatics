from quantamatics.core import settings
from quantamatics.core.APIClient import Session
from quantamatics.core.utils import QException, QLog
from quantamatics.data.fundamentals import FinancialStatement, CalendarPeriods


class Company:
    def __init__(self, companyID: int = None, instrumentID: int = None):
        if companyID is None and instrumentID is None:
            raise QException('Either Company or Instrument ID is required')
        session = Session()
        companyDF = session.apiWrapper(
            '/api/data/company/init',
            {
                'companyId': companyID,
                'instrumentId': instrumentID
            }
        )

        if len(companyDF) > 1:
            raise QException('More than one company selected')

        self.companyID = companyDF['company_id'].iat[0]
        self.companyName = companyDF['company_name'].iat[0]
        self.sectorName = companyDF['sector_name'].iat[0]
        self.industryName = companyDF['industry_name'].iat[0]
        self.industryGroupName = companyDF['industry_group_name'].iat[0]
        self.verticalName = companyDF['vertical_name'].iat[0]
        self.subverticalName = companyDF['subvertical_name'].iat[0]


class Instrument:
    def __init__(self, instrumentSymbol: str = None, instrumentSymbologyType: str = None,
                 preLoadLevel: int = 0, instrumentID: int = None, instrumentName: str = None):
        self.instrumentSymbol = instrumentSymbol
        self.instrumentID = instrumentID
        self.instrumentName = instrumentName
        self.instrumentSymbologyType = instrumentSymbologyType

        self._Company = None
        self._CalendarPeriods = None
        self._FinancialStatement = None
        self.logger = QLog()



        session = Session()
        instrumentDF = session.apiWrapper(
            '/api/data/instrument/load',
            {
                'instrumentSymbol': self.instrumentSymbol,
                'instrumentSymbologyType': self.instrumentSymbologyType,
                'instrumentId': self.instrumentID
            }
        )

        if len(instrumentDF) == 0:
            msg = 'No Instrument Found for Symbol: %s | ID: %s' % (self.instrumentSymbol, self.instrumentID)
            raise Exception(msg)

        if len(instrumentDF) > 1:
            msg = 'Critical - More than one Instrument Found for Symbol: %s | ID: %s' \
                  % (self.instrumentSymbol, self.instrumentID)
            self.logger.logDebug(instrumentDF)
            raise QException(msg)

        self.instrumentID = instrumentDF['instrument_id'].iat[0]
        self.instrumentSymbol = instrumentDF['symbol'].iat[0]
        self.instrumentName = instrumentDF['instrument_name'].iat[0]
        self.instrumentSector = instrumentDF['sector_name'].iat[0]
        self.instrumentIndustry = instrumentDF['industry_name'].iat[0]

        if preLoadLevel > 2:
            self._FinancialStatement = FinancialStatement(instrumentID=self.instrumentID)
        if preLoadLevel > 1:
            self._CalendarPeriods = CalendarPeriods(instrumentID=self.instrumentID)
        if preLoadLevel > 0:
            self._Company = Company(instrumentID=self.instrumentID)

    def getFinancialStatement(self):
        if self._FinancialStatement is None:
            self._FinancialStatement = FinancialStatement(instrumentID=self.instrumentID)
        return self._FinancialStatement

    def getCalendarPeriods(self):
        if self._CalendarPeriods is None:
            self._CalendarPeriods = CalendarPeriods(instrumentID=self.instrumentID)
        return self._CalendarPeriods

    def getCompany(self):
        if self._Company is None:
            self._Company = Company(instrumentID=self.instrumentID)
        return self._Company


    def getSymbology(self):
        if self.instrumentID is None:
            raise QException('No instrument defined')

        session = Session()
        symbologyDF = session.apiWrapper(
            '/api/data/instrument/getSymbology',
            {
                'instrumentId': self.instrumentID
            }
        )

        self.Symbology = dict()
        for itx in symbologyDF.iterrows():
            self.Symbology.update({itx[1].symbology_type: itx[1].symbol})
        return self.Symbology

    def getBrands(self, brandName: str = None):
        if self.instrumentID is None:
            msg = 'No instrument defined'
            raise QException(Exception(msg))

        session = Session()
        self.brandDF = session.apiWrapper(
            '/api/data/instrument/getBrands',
            {
                'instrumentId': self.instrumentID,
                'brandName': brandName
            }
        )
        return self.brandDF


class Universe:
    def __init__(self, sector: str = None, industry: str = None, instrumentSymbol: str = None,
                 instrumentSymbologyType: str = None, panelName: str = None):

        if instrumentSymbologyType is None:
            instrumentSymbologyType = settings.DefaultSymbologyType

        session = Session()
        self.UniverseDF = session.apiWrapper(
            '/api/data/universe/init',
            {
                'sector': sector,
                'industry': industry,
                'instrumentSymbol': instrumentSymbol,
                'instrumentSymbologyType': instrumentSymbologyType,
                'panelName': panelName
            }
        )

        if len(self.UniverseDF) == 0:
            msg = 'No Instruments Found'
            raise QException(msg)
