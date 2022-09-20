
__panelConfig = {
            'FacteusUSCPSummaryLatest': {'name':'USCP Summary v3.1','class':'Facteus.FacteusUSCPSummary'},
            'FacteusPulseBacktest': {'name':'Pulse - Backtest','class':'Facteus.FacteusPulseBacktest'},
            'FacteusPulse': {'name':'Pulse','class':'Facteus.FacteusPulse'},
            'FacteusUSCPSummaryDemo': {'name':'USCP Summary Demo','class':'Facteus.FacteusUSCPSummaryDemo'},
            'TenTenCreditFixedPanel':  {'name':'1010data Credit Fixed Panel','class':'TenTenData.TenTenCreditFixedPanel'},
            'TenTenDebitFixedPanel': {'name':'1010data Debit Fixed Panel','class':'TenTenData.TenTenDebitFixedPanel'},
            'TenTenCombinedFixedPanel': {'name':'1010data Combined Fixed Panel','class':'TenTenData.TenTenCombinedFixedPanel'},
            'TenTenCreditDenominatorPanel':  {'name':'1010data Credit Denominator Panel','class':'TenTenData.TenTenCreditDenominatorPanel'},
            'TenTenDebitDenominatorPanel': {'name':'1010data Debit Denominator Panel','class':'TenTenData.TenTenDebitDenominatorPanel'},
            'TenTenCombinedDenominatorPanel': {'name':'1010data Combined Denominator Panel','class':'TenTenData.TenTenCombinedDenominatorPanel'}
}

__panels = {}
__panelClasses = {}
for key in __panelConfig.keys():
    __panels[key]=__panelConfig[key]['name']
    __panelClasses[__panelConfig[key]['name']] = __panelConfig[key]['class']
