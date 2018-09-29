#!/usr/bin/python
from Venues.Base.Engine.engine import ClientBaseCmdTraits, VenueBaseCmdTraits
from Venues.Base.mffix.mffixvenue import MFFixVenue, MFFixVenueClient, MFFixVenueClientTraits
import datetime

class MDRequest(object):
    def __init__(self, msg):
        for t,v in msg:
            if '262' == t:
                self.MDReqID = v
            elif '263' == t:
                self.subReqType = v
            elif '265' == t:
                self.UpdateType = v
            elif '55' == t: # I know, this is part of the group though we always send from feed one request per one ccy pair
                self.ccy = v
            elif '9001' == t:
                self.QuoteSize = v

class BaseFixVenueClientFSS(MFFixVenueClient):
    def CreateInitialMDEntry(self, ccyN, tag52, tag64, bidPrices, ofrPrices):
        dnow = tag52[:8]
        tnow = tag52[9:]
        ccy = self.ccys[ccyN]
        rv = [
            ('262', None), # MDReqID will be provided when request arrived
            ('55',  ccy),
            ('64',  'SP'),
            ('268', str(len(bidPrices) + len(ofrPrices)))
            ]
        for i in range(len(bidPrices)):
            rv = rv + [
                        ('269', '0'), # MDEntryType BID(0)
                        ('279', '0'), # MDUpdateAction  NEW (0)
                        ('280', 'YSHBidMDEntryRefID' + str(ccyN*10 +i)),
                        ('290', str(i)), # Position
                        ('278', 'YSHBidMDEntryID' + str(ccyN*10 +i)),
                        ('270', bidPrices[i][0]),  # Price
                        ('271', bidPrices[i][1]),  # Size
                        ('272', dnow),
                        ('273', tnow),
                        ('15',  ccy[:3]),
                        ('282', None), # MDEntryOriginator, Must be filled when request arrives
                        ('64', tag64),
                 ]
        for i in range(len(ofrPrices)):
            rv = rv + [
                        ('269', '1'), # MDEntryType OFFER(1)
                        ('279', '0'), # MDUpdateAction  NEW (0)
                        ('280', 'YSHOfrMDEntryRefID' + str(ccyN*10 +i)),
                        ('290', str(i)), # Position
                        ('278', 'YSHOfrMDEntryID' + str(ccyN*10 +i)),
                        ('270', ofrPrices[i][0]),  # Price
                        ('271', ofrPrices[i][1]),  # Size
                        ('272', dnow),
                        ('273', tnow),
                        ('15',  ccy[:3]),
                        ('282', None), # MDEntryOriginator, Must be filled when request arrives
                        ('64', tag64),
                 ]
        return rv

    def UpdateInitialEntry(self, ccy, req):
        entry = self.initialMDEntries[ccy]
        for i in range(len(entry)):
            item = entry[i]
            if item[0] == '262':
                entry[i] = (item[0], req.MDReqID)

    def __init__(self, args):
        if not hasattr(self, 'name'): # Derived may set it up
            self.name = "Base Fix Client FSS" # Must be before super
        super(BaseFixVenueClientFSS, self).__init__(args)
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
        self.initialMDEntries = {}
        tag52 = self.GetTag52Value()
        tag64 = self.GetTag64Value()
        self.initialMDEntries = {
            self.ccys[0] : self.CreateInitialMDEntry( 0, # 'USD/JPY'
                               tag52, tag64,
                bidPrices=[('113.055','250000'), ('113.053','500000'),('113.047','1000000'),('113.034','2000000')],
                ofrPrices=[('113.088','250000'), ('113.091','500000'),('113.097','1000000'),('113.109','2000000')]
                    ),
            self.ccys[1] : self.CreateInitialMDEntry( 1, # 'EUR/USD'
                               tag52, tag64,
                bidPrices=[('1.20244','250000'), ('1.20242','500000'),('1.20235','1000000'),('1.20222','2000000')],
                ofrPrices=[('1.20266','250000'), ('1.20268','500000'),('1.20275','1000000'),('1.20289','2000000')]
                    ),
            self.ccys[2] : self.CreateInitialMDEntry( 2, # 'USD/CAD'
                               tag52, tag64,
                bidPrices=[('1.24064','250000'), ('1.24059','500000'),('1.24048','1000000'),('1.24035','2000000')],
                ofrPrices=[('1.24212','250000'), ('1.24218','500000'),('1.24228','1000000'),('1.24240','2000000')]
                    ),
            self.ccys[3] : self.CreateInitialMDEntry( 3, # 'USD/CHF'
                               tag52, tag64,
                bidPrices=[('0.97462','250000'), ('0.97457','500000'),('0.97449','1000000')],
                ofrPrices=[('0.97629','250000'), ('0.97633','500000'),('0.97641','1000000')]
                    ),
            self.ccys[4] : self.CreateInitialMDEntry( 4, # 'AUD/USD'
                               tag52, tag64,
                bidPrices=[('0.78499','500000'), ('0.78465','1000000'),('0.78439','2000000')],
                ofrPrices=[('0.78683','500000'), ('0.78722','1000000'),('0.78748','2000000')]
                    ),
            self.ccys[5] : self.CreateInitialMDEntry( 5, # 'CHF/JPY'
                               tag52, tag64,
                bidPrices=[('115.812','250000'), ('115.805','500000'),('115.79','1000000')],
                ofrPrices=[('116.023','250000'), ('116.03','500000'),('116.045','1000000')]
                    ),
            self.ccys[6] : self.CreateInitialMDEntry( 6, # 'EUR/CHF'
                               tag52, tag64,
                bidPrices=[('1.17223','250000'), ('1.17218','500000'),('1.17205','1000000')],
                ofrPrices=[('1.17377','250000'), ('1.17383','500000'),('1.17396','1000000')]
                    ),
            self.ccys[7] : self.CreateInitialMDEntry( 7, # 'EUR/GBP'
                               tag52, tag64,
                bidPrices=[('0.88646','250000'), ('0.88643','500000'),('0.88636','1000000'),('0.88622','2000000')],
                ofrPrices=[('0.88693','250000'), ('0.88696','500000'),('0.88703','1000000'),('0.88717','2000000')]
                    )
            }

    def IsCcySupported(self, ccy):
        for c in self.ccys:
            if c == ccy:
                return True
        return False

    def BuildSecListResponce(self, seqReqId, secListReqResult):
        currSecListRespID = self.currSecListRespID
        self.currSecListRespID = self.currSecListRespID + 1
        return self.BuildCommonPart('y') + [
                ('320', seqReqId),
                ('322', str(currSecListRespID)),
                ('560', secListReqResult)
            ]

    def RejectSecListRequest( self, (lport, rport), seqReqId):
        self.Log("RejectSecListRequest on {l},{r}".format(l = lport, r = rport))
        self.SendFixDataToServerAsVenue({'lport' : lport, 'rport' : rport, VenueBaseCmdTraits.DATA_FIELD_NAME : self.BuildSecListResponce(seqReqId, '1')})

    def SendSecList(self, (lport, rport), seqReqId):
        self.Log("SendSecList on {l},{r}".format(l = lport, r = rport))
        self.SendFixDataToServerAsVenue(
                {
                    'lport' : lport,
                    'rport' : rport,
                    VenueBaseCmdTraits.DATA_FIELD_NAME  : self.BuildSecListResponce(seqReqId, '0') + [
                            ('893', 'Y'),
                            # Now below is what in YSHTestExchange, MarketFactoryConfig.xml or whatever you need
                            ('146', '8'), # Only 8 markets in above exchange
                            ('55', self.ccys[0]),
                            ('167', 'FX'),
                            ('55', self.ccys[1]),
                            ('167', 'FX'),
                            ('55', self.ccys[2]),
                            ('167', 'FX'),
                            ('55', self.ccys[3]),
                            ('167', 'FX'),
                            ('55', self.ccys[4]),
                            ('167', 'FX'),
                            ('55', self.ccys[5]),
                            ('167', 'FX'),
                            ('55', self.ccys[6]),
                            ('167', 'FX'),
                            ('55', self.ccys[7]),
                            ('167', 'FX'),
                        ]
                }
            )

    def OnSecurityListRequest(self, (lport, rport), data):
        self.Log("OnSecurityListRequest on {l},{r}".format(l = lport, r = rport))
        for t,v in data:
            if '320' == t:
                seqReqId = v
            elif '559' == t:
                secListReqType = v
            elif '541' == t:
                maturity = v
        if maturity != 'SP' or secListReqType != '4':
            self.RejectSecListRequest((lport, rport), seqReqId)
        else:
            self.SendSecList((lport, rport), seqReqId)

    def SendMDRequestReject((lport, rport), req, rjReason):
        self.Log("SendMDRequestReject on {l},{r}".format(l = lport, r = rport))
        self.SendFixDataToServerAsVenue(
                {
                    'lport' : lport,
                    'rport' : rport,
                    VenueBaseCmdTraits.DATA_FIELD_NAME  : self.self.BuildCommonPart('Y') + [
                            ('281', rjReason)
                        ]
                }
            )

    def SendMDSnapshot(self, (lport, rport), req):
        self.Log("SendMDSnapshot on {l},{r}".format(l = lport, r = rport))
        answer = {'lport' : lport, 'rport' : rport}
        answer[VenueBaseCmdTraits.DATA_FIELD_NAME] = self.BuildCommonPart('W') + [
                            ('262', req.MDReqID),
                            ('55', req.ccy),
                            ('64', 'SP'),
                            ('268', '0')
                        ]
        self.SendFixDataToServerAsVenue(answer)
        self.UpdateInitialEntry(req.ccy, req)
        answer[VenueBaseCmdTraits.DATA_FIELD_NAME] = self.BuildCommonPart('X') + self.initialMDEntries[req.ccy]
        self.SendFixDataToServerAsVenue(answer)

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
            self.OnMDHeartBeat(answer, rv)
        elif msgType == 'V':
            self.OnMarketDataRequest((lport, rport), rv)
        elif msgType == 'x': # Security List Request
            self.OnSecurityListRequest((lport, rport), rv)
        else:
            self.ScenarioTradeFlowData((lport, rport), rv)

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

    def ScenarioTradeFlowData(self, (lport, rport), data):
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
    cl = BaseFixVenueClientFSS(args)
    cl.StartVenueProcess(venue)
    cl.StartVenueService()
    cl.PumpOutData()
    cl.StopVenueService()
    cl.StopVenueProcess()
    cl.PumpOutData()
    print "After cl.StopVenueProcess"


if __name__ == "__main__":
    main()

