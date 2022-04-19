from quantamatics.core import settings
from quantamatics.core.utils import QException

from quantamatics.providers import TenTenData,Facteus


class PanelFactory:
    def __init__(self):
        self.panelClasses = settings.PanelClasses

    def getPanel(self, panelName=None):
        try:
            panelClass = self.panelClasses[panelName]
        except:
            raise QException('Unknown Panel Specification: %s' % panelName)

        try:
            panelClass = self.panelClasses[panelName]
            return eval(panelClass)()
        except Exception as e:
            raise QException('Unable to Load Panel Class: %s. Error: %s' % (panelClass, str(e)))
