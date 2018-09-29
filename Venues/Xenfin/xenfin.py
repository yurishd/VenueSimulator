#!/usr/bin/python
from Venues.Base.Engine.engine import ClientBaseCmdTraits, VenueBaseCmdTraits
from Venues.Base.mffix.mffixvenue import MFFixVenue, MFFixVenueClient, MFFixVenueClientTraits
import datetime

class MDRequest(object):
    def __init__(self, msg):
        self.mdEntryTypes=[]
        for t,v in msg:
            if '262' == t:
                self.MDReqID = v
            elif '263' == t:
                self.subReqType = v
            elif '55' == t: # I know, this is part of the group though we always send from feed one request per one ccy pair
                self.ccy = v
            elif '269' == t:
                self.mdEntryTypes.append(v)

class BaseFixVenueClientXenfin(MFFixVenueClient):
    def CreateInitialMDEntry(self, ccyN, bidPrices, ofrPrices):
        ccy = self.ccys[ccyN]
        rv = [
            ('262', None), # MDReqID will be provided when request arrived
            ('268', str(len(bidPrices) + len(ofrPrices)))
            ]
        if (len(bidPrices) > 0):
            rv = rv + [
                        ('279', '0'), # 0 = New entry
                        ('269', '0'), # MDEntryType BID(0)
                        ('278', '{ccy}BidEntry0'.format(ccy=ccy)), # MDEntryID
                        ('55', ccy),
                        ('270', bidPrices[0][0]),  # Price
                        ('271', bidPrices[0][1]),  # Size
                        ('299',  '{ccy}BidQuoteID'.format(ccy=ccy)), # QuoteID
                        ('290', str(0)), # Position
                        ('93636','0.00'),
                        ('93637','0.00')
                 ]

        for i in range(1, len(bidPrices)):
            rv = rv + [
                        ('279', '0'), # 0 = New entry
                        ('269', '0'), # MDEntryType BID(0)
                        ('278', '{ccy}BidEntry{i}'.format(ccy=ccy, i=i)), # MDEntryID
                        ('270', bidPrices[i][0]),  # Price
                        ('271', bidPrices[i][1]),  # Size
                        ('299',  '{ccy}BidQuoteID'.format(ccy=ccy)), # QuoteID
                        ('290', str(0)), # Position
                        ('93636','0.00'),
                        ('93637','0.00')
                 ]
        for i in range(len(ofrPrices)):
            rv = rv + [
                        ('279', '0'), # 0 = New entry
                        ('269', '1'), # MDEntryType OFFER(1)
                        ('278', '{ccy}OfrEntry{i}'.format(ccy=ccy, i=i)), # MDEntryID
                        ('270', ofrPrices[i][0]),  # Price
                        ('271', ofrPrices[i][1]),  # Size
                        ('299',  '{ccy}OfrQuoteID'.format(ccy=ccy)), # QuoteID
                        ('290', str(0)), # Position
                        ('93636','0.00'),
                        ('93637','0.00')
                 ]
        return rv

    def UpdateInitialEntry(self, ccy, req):
        entry = self.initialMDEntries[ccy]
        for i in range(len(entry)):
            item = entry[i]
            if item[0] == '262':
                entry[i] = (item[0], req.MDReqID)

    def BuildDescArray(self, ccy, minTradeVol, maxTradeVol, pipSize):
        return [
            ('55',  ccy),
            ('167', 'FOR'), # SecurityType Foreign Exchange Contract (FOR)
            ('562', minTradeVol),
            ('1140',maxTradeVol),
            ('15',  ccy[:3]), # Currency
            ('9362',ccy[4:]), # Currency2
            ('9361',pipSize),
            ('9363','1'),     # Tradeable
            ]

    def __init__(self, args):
        if not hasattr(self, 'name'): # Derived may set it up
            self.name = "Base Fix Client RBS" # Must be before super
        super(BaseFixVenueClientXenfin, self).__init__(args)
        self.currSecListRespID = 1
        self.mdRequests = {} # Key is the ccy pair
        self.ccys = [
                'USD/JPY',
                'EUR/USD',
                'USD/CAD',
                'USD/CHF',
                'AUD/USD',
                'CHF/JPY',
                'EUR/CHF',
                'EUR/GBP',
            ]
        self.ccyDesc = {
            self.ccys[0] : self.BuildDescArray(self.ccys[0], '0.00', '50000000.00', '0.01'),
            self.ccys[1] : self.BuildDescArray(self.ccys[1], '0.00', '50000000.00', '0.0001'),
            self.ccys[2] : self.BuildDescArray(self.ccys[2], '0.00', '25000000.00', '0.0001'),
            self.ccys[3] : self.BuildDescArray(self.ccys[3], '0.00', '25000000.00', '0.0001'),
            self.ccys[4] : self.BuildDescArray(self.ccys[4], '0.00', '50000000.00', '0.0001'),
            self.ccys[5] : self.BuildDescArray(self.ccys[5], '0.00', '25000000.00', '0.01'),
            self.ccys[6] : self.BuildDescArray(self.ccys[6], '0.00', '25000000.00', '0.0001'),
            self.ccys[7] : self.BuildDescArray(self.ccys[7], '0.00', '50000000.00', '0.0001')
        }
        self.initialMDEntries = {}
        self.initialMDEntries = {
            self.ccys[0] : self.CreateInitialMDEntry( 0, # 'USD/JPY'
                bidPrices=[('113.055','250000'), ('113.053','500000'),('113.047','1000000'),('113.034','2000000')],
                ofrPrices=[('113.088','250000'), ('113.091','500000'),('113.097','1000000'),('113.109','2000000')]
                    ),
            self.ccys[1] : self.CreateInitialMDEntry( 1, # 'EUR/USD'
                bidPrices=[('1.20244','250000'), ('1.20242','500000'),('1.20235','1000000'),('1.20222','2000000')],
                ofrPrices=[('1.20266','250000'), ('1.20268','500000'),('1.20275','1000000'),('1.20289','2000000')]
                    ),
            self.ccys[2] : self.CreateInitialMDEntry( 2, # 'USD/CAD'
                bidPrices=[('1.24064','250000'), ('1.24059','500000'),('1.24048','1000000'),('1.24035','2000000')],
                ofrPrices=[('1.24212','250000'), ('1.24218','500000'),('1.24228','1000000'),('1.24240','2000000')]
                    ),
            self.ccys[3] : self.CreateInitialMDEntry( 3, # 'USD/CHF'
                bidPrices=[('0.97462','250000'), ('0.97457','500000'),('0.97449','1000000')],
                ofrPrices=[('0.97629','250000'), ('0.97633','500000'),('0.97641','1000000')]
                    ),
            self.ccys[4] : self.CreateInitialMDEntry( 4, # 'AUD/USD'
                bidPrices=[('0.78499','500000'), ('0.78465','1000000'),('0.78439','2000000')],
                ofrPrices=[('0.78683','500000'), ('0.78722','1000000'),('0.78748','2000000')]
                    ),
            self.ccys[5] : self.CreateInitialMDEntry( 5, # 'CHF/JPY'
                bidPrices=[('115.812','250000'), ('115.805','500000'),('115.79','1000000')],
                ofrPrices=[('116.023','250000'), ('116.03','500000'),('116.045','1000000')]
                    ),
            self.ccys[6] : self.CreateInitialMDEntry( 6, # 'EUR/CHF'
                bidPrices=[('1.17223','250000'), ('1.17218','500000'),('1.17205','1000000')],
                ofrPrices=[('1.17377','250000'), ('1.17383','500000'),('1.17396','1000000')]
                    ),
            self.ccys[7] : self.CreateInitialMDEntry( 7, # 'EUR/GBP'
                bidPrices=[('0.88646','250000'), ('0.88643','500000'),('0.88636','1000000'),('0.88622','2000000')],
                ofrPrices=[('0.88693','250000'), ('0.88696','500000'),('0.88703','1000000'),('0.88717','2000000')]
                    )
            }

    def IsCcySupported(self, ccy):
        for c in self.ccys:
            if c == ccy:
                return True
        return False


    def SendMDRequestReject((lport, rport), req, rjReason):
        self.Log("SendMDRequestReject on {l},{r}".format(l = lport, r = rport))
        self.SendFixDataToServerAsVenue(
                {
                    'lport' : lport,
                    'rport' : rport,
                    VenueBaseCmdTraits.DATA_FIELD_NAME : self.BuildCommonPart('Y') + [
                            ('281', rjReason)
                        ]
                }
            )

    def SendMDSnapshot(self, (lport, rport), req):
        self.Log("SendMDSnapshot on {l},{r}".format(l = lport, r = rport))
        answer = {'lport' : lport, 'rport' : rport}
        self.UpdateInitialEntry(req.ccy, req)
        answer[VenueBaseCmdTraits.DATA_FIELD_NAME] = self.BuildCommonPart('X') + self.initialMDEntries[req.ccy]
        self.SendFixDataToServerAsVenue(answer)

    def OnSecurityListRequest(self, (lport, rport), data):
        for t,v in data:
            if '320' == t:
                seqReqId=v;
            if '559' == t:
                if '4' != v:
                    self.Log("Unexpected SequrityListRequestType [{tp}]. Expected 4(ALL_SECURITIES)". format(tp=v))
        seclist = []
        for ccy in self.ccys:
            seclist = seclist + self.ccyDesc[ccy]

        self.SendFixDataToServerAsVenue( {
            'lport' : lport,
            'rport' : rport,
            VenueBaseCmdTraits.DATA_FIELD_NAME : self.BuildCommonPart('y') +
                    [ ('320',seqReqId), # SecurityReqID
                     ('322',seqReqId),  # SecurityResponseID
                     ('560', '0'),      # SecurityRequestResult  VALID_REQUEST (0)
                     ('146', str(len(self.ccys)))] + # NoRelatedSym
                    seclist
            })
        

    def OnMarketDataRequest(self, (lport, rport), data):
        req = MDRequest(data)
        if self.IsCcySupported(req.ccy):
            self.mdRequests[req.ccy] = req
            self.SendMDSnapshot((lport, rport), req)
        else:
            self.SendMDRequestReject((lport, rport), req, '0')

    def OnMDHeartBeat(self, (lport, rport), rv):
        # Nothing to do... just ping back
        answer = {'lport' : lport, 'rport' : rport}
        answer[VenueBaseCmdTraits.DATA_FIELD_NAME] = rv
        self.SendFixDataToServerAsVenue(answer)

    def OnMarketData(self, (lport, rport), data):
        rv = data[:] # Deep copy
        msgType = rv[0][1]
        self.Log("OnMarketData on {l},{r}, {m}".format(l = lport, r = rport, m=msgType))
        if msgType == '0': # Heartbeat
            self.OnMDHeartBeat((lport, rport), rv)
        elif msgType == 'V':
            self.OnMarketDataRequest((lport, rport), rv)
        elif msgType == 'x': # Security List Request
            self.OnSecurityListRequest((lport, rport), rv)
        else:
            self.ScenarioMarketData((lport, rport), rv)

    def OnTradeHeartBeat(self, (lport, rport), rv):
        # Nothing to do... just ping back
        answer = {'lport' : lport, 'rport' : rport}
        answer[VenueBaseCmdTraits.DATA_FIELD_NAME] = rv
        self.SendFixDataToServerAsVenue(answer)

    def OnTradeFlowData(self, (lport, rport), data):
        rv = data[:] # Deep copy
        msgType = rv[0][1]
        self.Log("OnTradeFlowData on {l},{r}, {m}".format(l = lport, r = rport, m=msgType))
        if msgType == '0': # Heartbeat
            self.OnTradeHeartBeat((lport, rport), rv)
        else:
            self.ScenarioTradeFlowData((lport, rport), rv)

    def SendMDSessionOpen(self):
        self.Log("SendMDSessionOpen is CALLED");
        if hasattr(self, 'MDOpen'):
            self.Log("SendSessionOpen for MD is SENDING");
            self.SendFixDataToServerAsVenue(self.MDOpen)


    def SendOFSessionOpen(self):
        self.Log("SendOFSessionOpen is CALLED");
        if hasattr(self, 'OFOpen'):
            self.Log("SendSessionOpen for OF is SENDING");
            self.SendFixDataToServerAsVenue(self.OFOpen)

    def NoticeMDLoggedOn(self, (lport, rport)):
        self.MDOpen = {
                    'lport' : lport,
                    'rport' : rport,
                    VenueBaseCmdTraits.DATA_FIELD_NAME  : self.BuildCommonPart('h') + [
                            ('336', 'XENFINDIRECT_YSH'),
                            ('340', '2'), # TradSesStatus OPEN (2)
                            ('325', 'Y')  # UnsolicitedIndicator    Y
                        ]
                }
        self.SendMDSessionOpen()

    def NoticeOFLoggedOn(self, (lport, rport)):
        self.OFOpen = {
                    'lport' : lport,
                    'rport' : rport,
                    VenueBaseCmdTraits.DATA_FIELD_NAME : self.BuildCommonPart('h') + [
                            ('336', 'iXENFINDIRECT_YSH'),
                            ('340', '2'), # TradSesStatus OPEN (2)
                            ('325', 'Y')  # UnsolicitedIndicator    Y
                        ]
                }
        self.SendOFSessionOpen()

    def ScenarioTradeFlowData(self, (lport, rport), data):
        answer = {'lport' : lport, 'rport' : rport}
        self.Log("ScenarioTradeFlowData on ports {l},{r}".format(l = lport, r = rport))

    def ScenarioMarketData(self, (lport, rport), data):
        self.Log("ScenarioMarketData on ports {l},{r}".format(l = lport, r = rport))

    def OnDataDone(self):
        self.Log("OnDataDone. Scenario Active")
        return True

def main():
    args = {
        ClientBaseCmdTraits.PORTS_SET_NAME        : ['36914','36912'],
        MFFixVenueClientTraits.USERIDS_TUPLE_NAME : ('YSH-TEST-MD', 'YSH-TEST-OF')
    }
    venue = MFFixVenue()
    cl = BaseFixVenueClientXenfin(args)
    cl.StartVenueProcess(venue)
    cl.StartVenueService()
    cl.PumpOutData()
    cl.StopVenueService()
    cl.StopVenueProcess()
    cl.PumpOutData()
    print "After cl.StopVenueProcess"


if __name__ == "__main__":
    main()

