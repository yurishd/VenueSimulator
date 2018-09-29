#!/usr/bin/python
from Venues.Base.Engine.engine import VenueBaseCmdProcessor, ClientBaseCmdProcessor, ClientBaseCmdTraits, VenueBaseCmdTraits
from ouchprotocol import OuchProtocol
import datetime
from collections import deque
import pdb


#######################################################
# Currently there is no specific command back and forth
# ouch venue/ ouch client
#######################################################
class OuchVenueTraits(object):
    OUCH_VENUE_CMD_DOMAIN="OUCH_VENUE_CMDS"
    def __init__(self):
        pass

class OuchVenueClientTraits(object):
    OUCH_CLIENT_CMD_DOMAIN = "OUCH_CLIENT_CMDS"
    USERIDS_TUPLE_NAME     = "UserIDs"
    def __init__(self):
        pass

class OuchVenue(VenueBaseCmdProcessor):
    def __init__(self):
        # self.name must be initialized before parent constructor called
        self.name = "Ouch Venue"
        super(OuchVenue, self).__init__()
        self.debug = False

    def AboutToStartServer(self):
        self.protocol = OuchProtocol(self) # self as a logger

class OuchVenueClientDataProc(object):
    EXPECTED_REQID_MODULO = 10000

    def __init__(self, args):
        pass

    def _OnLOGON(self, data, desc):
        lport, rport = desc['ports']
        if not 'userid' in data:
            raise Exception('No userid in data')
        userid = data['userid']
        d = {'lport':lport,'rport':rport, VenueBaseCmdTraits.DATA_FIELD_NAME:data}
        if self.rconn['md']['userid'] ==  userid:
            self.rconn['md']['ports'] = (lport, rport)
            self.rconn['md']['seqN'] = 1
            self.conn[(lport, rport)] = 'md'
        elif self.rconn['of']['userid'] ==  userid:
            self.rconn['of']['ports'] = (lport, rport)
            self.rconn['of']['seqN'] = 1
            self.conn[(lport, rport)] = 'of'
        else:
            raise Exception('Unexpected userid ({})'.format(userid))
        data['seqN'] = str(1)
        self.SendDataToServerFromVenue(d)
        return True; 

    def _OnLOGOUT(self, data, desc):
        lport, rport = desc['ports']
        return True; 

    def _OnHEARTBEAT(self, data, desc):
        lport, rport = desc['ports']
        return True; 

    def _OnTEST_REQUEST(self, data, desc):
        lport, rport = desc['ports']
        return True; 

    def _OnTEST_RESPONSE(self, data, desc):
        lport, rport = desc['ports']
        return True; 

    def _OnREJECT(self, data, desc):
        lport, rport = desc['ports']
        return True; 

    def _OnINSTRUMENT_INFO_REQUEST(self, data, desc):
        lport, rport = desc['ports']
        self.Log("_OnINSTRUMENT_INFO_REQUEST {},{},{}".format(lport, rport, data))
        self.msgs.insert(0, (data, desc))
        return False; 

    def _OnINSTRUMENT_INFO(self, data, desc):
        lport, rport = desc['ports']
        return True; 

    def _OnINSTRUMENT_INFO_ACK(self, data, desc):
        lport, rport = desc['ports']
        return True; 

    def _OnQUOTE(self, data, desc):
        lport, rport = desc['ports']
        iiid = int(data["instinfoID"])
        self.quotes[ iiid ]['quotes'].appendleft({"quoteID": data["quoteID"],
                                                                     "rate": data["rate"],
                                                                     "amount": data["amount"]})
        while len(self.quotes[ iiid ]['quotes']) > 64:
            self.quotes[ iiid ]['quotes'].pop()
        return False; 

    def _OnQUOTE_CANCEL(self, data, desc):
        lport, rport = desc['ports']
        return True; 

    def _OnMASS_QUOTE(self, data, desc):
        lport, rport = desc['ports']
        return True; 

    def _OnNEW_ORDER(self, data, desc):
        lport, rport = desc['ports']
        return True; 

    def _OnEXECUTION_REPORT(self, data, desc):
        lport, rport = desc['ports']
        clOrdId = int(data['clOrdID'])
        iiid = clOrdId - clOrdId/self.EXPECTED_REQID_MODULO * self.EXPECTED_REQID_MODULO
        if self.orders[iiid][-1]['status'] == 'sent':
            self.orders[iiid][-1]['status'] = 'exrpt'
            self.orders[iiid][-1]['execID'] = data['execID']
        return True; 

    def _OnEXECUTION_REPORT_REJECT(self, data, desc):
        lport, rport = desc['ports']
        clOrdId = int(data['clOrdID'])
        iiid = clOrdId - clOrdId/self.EXPECTED_REQID_MODULO * self.EXPECTED_REQID_MODULO
        if self.orders[iiid][-1]['status'] == 'sent':
            self.orders[iiid][-1]['status'] = 'exrpt_reject'
            self.orders[iiid][-1]['errCode'] = data['errCode']
        return True; 

    def _OnEXECUTION_REPORT_ACK(self, data, desc):
        lport, rport = desc['ports']
        return True; 

    def _OnEXECUTION_REPORT_NACK(self, data, desc):
        lport, rport = desc['ports']
        return True; 

    def _OnEXECUTION_REPORT_REJECT_ACK(self, data, desc):
        lport, rport = desc['ports']
        return True; 

    def _OnEXECUTION_REPORT_REJECT_NACK(self, data, desc):
        lport, rport = desc['ports']
        return True; 

    def _OnTRADE_TIMED_OUT(self, data, desc):
        lport, rport = desc['ports']
        return True; 

    def _OnDONT_KNOW_TRADE(self, data, desc):
        lport, rport = desc['ports']
        return True; 

def SettleTS():
    t = datetime.date.today() - datetime.date(1970,1,1)
    return t.days * 86400000 

class OuchVenueClient(ClientBaseCmdProcessor, OuchVenueClientDataProc):
    OnDataDescriptor = {
    'A':OuchVenueClientDataProc._OnLOGON,
    'B':OuchVenueClientDataProc._OnLOGOUT,
    'C':OuchVenueClientDataProc._OnHEARTBEAT,
    '1':OuchVenueClientDataProc._OnTEST_REQUEST,
    '0':OuchVenueClientDataProc._OnTEST_RESPONSE,
    'K':OuchVenueClientDataProc._OnREJECT,
    'D':OuchVenueClientDataProc._OnINSTRUMENT_INFO_REQUEST,
    'E':OuchVenueClientDataProc._OnINSTRUMENT_INFO,
    'F':OuchVenueClientDataProc._OnINSTRUMENT_INFO_ACK,
    'h':OuchVenueClientDataProc._OnQUOTE,
    'Z':OuchVenueClientDataProc._OnQUOTE_CANCEL,
    'i':OuchVenueClientDataProc._OnMASS_QUOTE,
    'l':OuchVenueClientDataProc._OnNEW_ORDER,
    'm':OuchVenueClientDataProc._OnEXECUTION_REPORT,
    'W':OuchVenueClientDataProc._OnEXECUTION_REPORT_REJECT,
    't':OuchVenueClientDataProc._OnEXECUTION_REPORT_ACK,
    'u':OuchVenueClientDataProc._OnEXECUTION_REPORT_NACK,
    'v':OuchVenueClientDataProc._OnEXECUTION_REPORT_REJECT_ACK,
    'w':OuchVenueClientDataProc._OnEXECUTION_REPORT_REJECT_NACK,
    'U':OuchVenueClientDataProc._OnTRADE_TIMED_OUT,
    'T':OuchVenueClientDataProc._OnDONT_KNOW_TRADE,
    }
    def __init__(self, args):
        if not hasattr(self, 'name'):
            self.name = "Ouch Client"
        super(OuchVenueClient, self).__init__(args)

        # Ouch specific values
        uids = args[OuchVenueClientTraits.USERIDS_TUPLE_NAME]
        self.SetUserIDs(uids[0], uids[1]) # 

        # Below controls behavior of based classes
        self.to = 2 # default 5. Timeout to expect any message from Feed (through venue).
        # self.debug = True

        self.msgs = []
        self.currInstrInfoID = 0
        self.quotes = {}
        self.orders = {}

    def OnMsgFromVenue(self, (lport, rport), data):
        try:
            msgtype = data['msgtype']
        except KeyError as e:
            s = "Fail extract msgtype"
            for mk, mv in kwargs.items():
                s = s + ", {}:{}".format(mk,mv)
            self.Log(s)
            raise
           
        if not (lport, rport) in self.conn:
            raise Exception('Data on unrecorded connection ({}), ({})'.format(lport, rport))

        if 'A' == msgtype:
            rv = self.OnDataDescriptor[msgtype](self, data, { 'ports':(lport, rport)})
            connName = self.conn[(lport, rport)]
            if connName in self.rconn:
                desc = self.rconn[connName]
                desc['seqNRx'] = int(data['seqN'])
            return rv
        else:
            connName = self.conn[(lport, rport)]
            desc = self.rconn[connName]
            if int(data['seqN']) != desc['seqNRx']+1:
                raise Exception("Unexpected incoming sequence number {}. Expected {} on {} connection".format(
                                                            int(data['seqN']), desc['seqNRx']+1), connName)
            desc['seqNRx'] = int(data['seqN'])
            return self.OnDataDescriptor[msgtype](self, data, desc)

    def InitMsg(self, desc, outmsg):
        lport, rport = desc['ports']
        rv = {'lport':lport, 'rport':rport, VenueBaseCmdTraits.DATA_FIELD_NAME:outmsg}
        connName = self.conn[(lport, rport)]
        seqN = self.rconn[connName]['seqNSnd'] + 1
        self.rconn[connName]['seqNSnd'] = seqN
        outmsg['seqN'] = str(seqN)
        d  = datetime.date.today()
        td = datetime.datetime.today() - datetime.datetime(d.year, d.month, d.day)
        outmsg['timestamp'] = str(td.seconds * 1000)
        return rv

    def SendInstrumentInfo(self, conndesc, info):
        iiid = self.currInstrInfoID + 1
        self.currInstrInfoID = iiid
        info['instinfoID'] = str(iiid)

        rv = self.InitMsg(conndesc, info)
        rv[VenueBaseCmdTraits.DATA_FIELD_NAME]['msgtype'] = 'E' # INSTRUMENT_INFO
        self.SendDataToServerFromVenue(rv)
        self.quotes[iiid] = {'request':rv, 'quotes' : deque()}

    def SendNewOrder(self, iiid, clOrdID, ordAmnt, QuoteID, allowBadQuoteID=False):
        req = self.quotes[iiid]['request'][VenueBaseCmdTraits.DATA_FIELD_NAME]
        conndesc = self.rconn['of']
        quotes = self.quotes[iiid]['quotes']
        for qdesc in quotes:
            if qdesc['quoteID'] == str(QuoteID):
                break;
        else:
            self.Log("QuoteID {} is not found".format(QuoteID))
            if not allowBadQuoteID:
                return True
        
        data = {
                'clOrdID': clOrdID,
                'instID' : req['instID'],
                'side'   : req['side'],
                'ordMinAmt' : str(ordAmnt),
                'ordAmt' : str(ordAmnt),
                'streamrefID':req['streamrefID'],
                'quoteID':str(QuoteID),
                'px':qdesc['rate'],
                'ordType': 'D', # PREVIOUSLY QUOTED
                'clntID': 'YSH_MYSELF',
                'IsBaseSpecified': '1',
                'execFirm':'YSH Solutions',
                'settleTS':str(SettleTS())
               }
        rv = self.InitMsg(conndesc, data)
        rv[VenueBaseCmdTraits.DATA_FIELD_NAME]['msgtype'] = 'l' # NEW ORDER
        self.SendDataToServerFromVenue(rv)

    def SendExrptAck(self, iiid, order):
        conndesc = self.rconn['of']
        data = {
                'clOrdId'     : order['clOrdId'],
                'execOrdId'   : order['execID'],
                'isAggressor' : '2', # False
                'LMUID'       : 'YS_LMUID'
               }
        rv = self.InitMsg(conndesc, data)
        rv[VenueBaseCmdTraits.DATA_FIELD_NAME]['msgtype'] = 't' # EXECUTION_REPORT_ACK
        self.SendDataToServerFromVenue(rv)

    def SendExrptRejectAck(self, iiid, order):
        conndesc = self.rconn['of']
        data = {
                'clOrdId'     : order['clOrdId'],
                'ExecutionID' : 'YS_TEST_TRADER_ID',
                'LMUID'       : 'YS_LMUID'
               }
        rv = self.InitMsg(conndesc, data)
        rv[VenueBaseCmdTraits.DATA_FIELD_NAME]['msgtype'] = 'v' # EXECUTION_REPORT_REJECT_ACK
        self.SendDataToServerFromVenue(rv)

def test():
    venue = OuchVenue()
    cl = OuchVenueClient({ OuchVenueClientTraits.USERIDS_TUPLE_NAME : ('marketdata', 'orderflow'), ClientBaseCmdTraits.PORTS_SET_NAME:['36912', '36914']})

if __name__ == "__main__":
    test()
