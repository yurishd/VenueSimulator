#!/usr/bin/python
from Venues.Base.Engine.engine import VenueBaseCmdTraits, ClientBaseCmdTraits
from Venues.Base.itch.itchvenue import ItchVenue, ItchVenueClient, ItchClientTraits
from Venues.Base.mffix.mffixvenue import MFFixVenueClientTraits
from Venues.Base.itch.itchprotocol import ItchProtocol, MESSAGE_TYPE, \
        MsgTicker, MsgMarketSnapshot, MsgCancelOrder, MsgModifyOrder, MsgNewOrder, MsgInstDirRequest,     \
        MsgMDUnsubsRequest, MsgMDSubsRequest, MsgTickerUnsubsRequest, MsgTickerSubsRequest,               \
        MsgMarketSnapshotRequest, MsgClientHeartBeat, MsgLogoutRequest, MsgLoginRequest, MsgInstDirectory,\
        MsgErrorNotification, MsgServerHeartBeat, MsgSequencedData, MsgLoginRejected, MsgLoginAccepted
import datetime

class MarketSnapshotOrderBuilder(object):
    def __init__(self, amount, minqty, lotsz, ordID):
        self.amount = amount
        self.minqty = minqty
        self.lotsz  = lotsz
        self.ordID  = ordID

    def Build(self):
        return {
                MsgMarketSnapshot.Amount  : self.amount,
                MsgMarketSnapshot.MinQty  : self.minqty,
                MsgMarketSnapshot.LotSize : self.lotsz,
                MsgMarketSnapshot.OrderID : self.ordID
               }

class MarketSnapshotPxBuilder(object):
    def __init__(self, px):
        self.px = px
        self.orders = []

    def AddOrder(self, amount, ordID, minqty = "", lotsz = ""):
        self.orders.append(MarketSnapshotOrderBuilder(amount, minqty, lotsz, ordID));
        return self.orders[-1]

    def Build(self):
        return {
                MsgMarketSnapshot.Px     : self.px,
                MsgMarketSnapshot.Orders : [ord.Build() for ord in self.orders]
               }

class MarketSnapshotPairBuilder(object):
    def __init__(self, pairName):
        self.ccy = pairName
        self.bids = []
        self.ofrs = []

    def AddBid(self, px):
        self.bids.append(MarketSnapshotPxBuilder(px))
        return self.bids[-1]

    def AddOfr(self, px):
        self.bids.append(MarketSnapshotPxBuilder(px))
        return self.ofrs[-1]

    def Build(self):
        return {
                MsgMarketSnapshot.CurrPair : self.ccy,
                MsgMarketSnapshot.BidPxs   : [px.Build() for px in self.bids],
                MsgMarketSnapshot.OfrPxs   : [px.Build() for px in self.ofrs]
               }

class MarketSnapshotBuilder(object):
    def __init__(self):
        # self.payload = {MESSAGE_TYPE:'S',MsgMarketSnapshot.Pairs=[]}
        self.pairs = []

    def AddPair(self, pair):
        self.pairs.append(MarketSnapshotPairBuilder(pair))
        return self.pairs[-1]

    def Build(self):
        return {MESSAGE_TYPE:'S', MsgMarketSnapshot.Pairs : [pair.Build() for pair in self.pairs]}

class BaseItchVenueClientHotspotI(ItchVenueClient):
    def __init__(self, args):
        if not hasattr(self, 'name'): # Derived may set it up
            self.name = "Base Itch Client HotspotI" # Must be before super
        super(BaseItchVenueClientHotspotI, self).__init__(args)
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
        self.mdsBuilder = MarketSnapshotBuilder()

    def IsCcySupported(self, ccy):
        for c in self.ccys:
            if c == ccy:
                return True
        return False

    def SendInstrumentDirectory(self, lport, rport):
        '''
            This function is overwriting "empty" base function
            which is called on Instrument Directory request SendErrorAsHotspotVenue
        '''
        self.SendAsHotspotVenue( {
                MsgInstDirectory.MsgType : 'R',
                MsgInstDirectory.Pairs : [{MsgInstDirectory.CurrPair : ccy} for ccy in self.ccys]
                    } )
        return True

    def OnDataDone(self):
        self.Log("OnDataDone. Itch Scenario Active")
        return True

    def SendLogonResponse(self, data):
        if (data[MsgLoginRequest.LoginName].strip() != self.expected_user_name) or (
            data[MsgLoginRequest.LoginPass].strip() != self.expected_user_pass):
            self.Log("User login failed. Received [{ru}],[{rp}] while expected [{eu}],[{ep}]".format(
                    ru=data[MsgLoginRequest.LoginName].strip(),
                    rp=data[MsgLoginRequest.LoginPass].strip(),
                    eu=self.expected_user_name,
                    ep=self.expected_user_pass))
            self.SendAsHotspotVenue( {
                            MsgLoginRejected.MsgType : 'J',
                            MsgLoginRejected.Reason  : 'Bad name/passwrd'
                    })
            self.ItchLoggedIn = False 
            # self.SendEndOfSession()
        else:
            self.Log("Successfully logged in")
            self.SendAsHotspotVenue( {
                            MsgLoginAccepted.MsgType : 'A',
                            MsgLoginAccepted.SeqNumberReserved : '1'
                    })
            self.ItchLoggedIn = True
        return True
            
    def SendErrorNotification(self, err):
        self.SendAsHotspotVenue( {
                            MsgErrorNotification.MsgType : 'E',
                            MsgErrorNotification.ErrExplanation: err
                    })
        return True

    def SendMsgSequencedData(self, payload):
        tm = datetime.utcnow().time()
        ms = tm.microsecond / 1000
        self.SendAsHotspotVenue( {
                            MsgSequencedData.MsgType : 'S',
                            MsgSequencedData.Time    : '{t}{ms}'.format(t=tm.strftime('%H%M%S'), ms=ms),
                            MsgSequencedData.Payload : payload
                    })
        return True

 
def main():
    args = {
        ClientBaseCmdTraits.PORTS_SET_NAME : [
                                {   VenueBaseCmdTraits.EXCH_PORT_DESC_KEY_NAME: '36912',
                                    VenueBaseCmdTraits.EXCH_PORT_PROTO : {
                                        VenueBaseCmdTraits.EXCH_PORT_PROTO_CLASS_NAME:'ItchProtocol',
                                        VenueBaseCmdTraits.EXCH_PORT_PROTO_MODULE: 'Venues.Base.itch.itchprotocol',
                                        VenueBaseCmdTraits.EXCH_PORT_PROTO_CLASS_ARGS: {'includeOptionalField':True}
                                        }
                                },
                                {   VenueBaseCmdTraits.EXCH_PORT_DESC_KEY_NAME: '36914',
                                    VenueBaseCmdTraits.EXCH_PORT_PROTO : {
                                        VenueBaseCmdTraits.EXCH_PORT_PROTO_CLASS_NAME:'MFFixProtocol',
                                        VenueBaseCmdTraits.EXCH_PORT_PROTO_MODULE: 'Venues.Base.mffix.mffixprotocol',
                                        }
                                },
            ],
        ItchClientTraits.CREDENTIALS_NAME : ('YSHUserName', 'YSHUserPass'),
        # We setting MD userid as empty so engine will not even touch it, so no overwrite if other proto is using it
        MFFixVenueClientTraits.USERIDS_TUPLE_NAME : ('', 'YSH-TEST-OF')
    }
    venue = ItchVenue()
    cl = BaseItchVenueClientHotspotI(args)
    cl.StartVenueProcess(venue)
    cl.StartVenueService()
    cl.PumpOutData()
    cl.StopVenueService()
    cl.StopVenueProcess()
    cl.PumpOutData()
    print "After cl.StopVenueProcess"


if __name__ == "__main__":
    main()

