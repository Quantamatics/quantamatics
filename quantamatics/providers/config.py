
__panelConfig = {
            'FacteusUSCPSummaryLatest': {'name':'USCP Summary v3.1','class':'Facteus.FacteusUSCPSummary'},
            'FacteusUSCPSummaryDemo': {'name':'USCP Summary Demo','class':'Facteus.FacteusUSCPSummaryDemo'},
            'TenTenCreditFixedPanel':  {'name':'1010data Credit Fixed Panel','class':'TenTenData.TenTenCreditFixedPanel'},
            'TenTenDebitFixedPanel': {'name':'1010data Debit Fixed Panel','class':'TenTenData.TenTenDebitFixedPanel'},
            'TenTenCombinedFixedPanel': {'name':'1010data Combined Fixed Panel','class':'TenTenData.TenTenCombinedFixedPanel'}
}

__panels = {}
__panelClasses = {}
for key in __panelConfig.keys():
    __panels[key]=__panelConfig[key]['name']
    __panelClasses[__panelConfig[key]['name']] = __panelConfig[key]['class']