#!/usr/bin/python
import os
import sys
import inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir) 

from Engine.engine import VenueBaseCmdProcessor, ClientBaseCmdProcessor, ClientBaseCmdTraits, VenueBaseCmdTraits
from itchprotocol import ItchProtocol, MESSAGE_TYPE, \
        MsgTicker, MsgMarketSnapshot, MsgCancelOrder, MsgModifyOrder, MsgNewOrder, MsgInstDirRequest,     \
        MsgMDUnsubsRequest, MsgMDSubsRequest, MsgTickerUnsubsRequest, MsgTickerSubsRequest,               \
        MsgMarketSnapshotRequest, MsgClientHeartBeat, MsgLogoutRequest, MsgLoginRequest, MsgInstDirectory,\
        MsgErrorNotification, MsgServerHeartBeat, MsgSequencedData, MsgLoginRejected, MsgLoginAccepted 
#from vsfix.vsfixvenue import VSFixVenueClientProcessorHelper
import datetime
from collections import deque
import pdb


#######################################################
# Currently there is no specific command back and forth
# itch venue/ itch client
#######################################################
class ItchVenueTraits(object):
    ITCH_VENUE_CMD_DOMAIN="ITCH_VENUE_CMDS"

class ItchClientTraits(object):
    ITCH_CLIENT_CMD_DOMAIN = 'ITCH_CLIENT_CMDS'
    CREDENTIALS_NAME       = 'Credentials'

class ItchVenue(VenueBaseCmdProcessor):
    def __init__(self):
        # self.name must be initialized before parent constructor called
        self.name = "Itch Venue"
        super(ItchVenue, self).__init__()
        self.debug = False

    def AboutToStartServer(self):
        self.protocol = ItchProtocol(self) # self as a logger

   
class ItchVenueClientDataProc(object):

    def __init__(self, args):
        super(ItchVenueClientDataProc, self).__init__()
        self.ItchLoggedIn = False

    def printMsg(self, msgType, msg, trailer = 'OVERWRITE ME!!!'):
        m = msgType.__name__
        for a in dir(msgType):
            if not a.startswith('__'):
                m = m + ', [{n}:{v}]'.format(n=a, v=msg[getattr(msgType,a)])
        self.Log('New message arrived: {m}. {t}'.format(m=m, t=trailer))
            
    def SendEndOfSession(self):
        self.SendAsHotspotVenue( {
                   MsgSequencedData.MsgType : 'S',
                   MsgSequencedData.Payload : ''
               } )
        self.ItchLoggedIn = False
        return True

    def SendLogonResponse(self, data):
        # Function should be overwritten if needed
        # May test name/pass here
        # # May respond as MsgLoginRejected:
        # d = {
        #           MsgLoginRejected.MsgType : 'J',
        #           MsgLoginRejected.Reason  : 'Just test rejection'
        #     }
        # # or MsgLoginAccepted:
        # d = {
        #           MsgLoginAccepted.MsgType : 'A',
        #           MsgLoginAccepted.SeqNumberReserved : '1' # Currently MUST be 1
        #     }
        # # Finally, just send it back
        # self.SendAsHotspotVenue(d)
        # Though we will send by default Accepted
        self.SendAsHotspotVenue( {
                            MsgLoginAccepted.MsgType : 'A',
                            MsgLoginAccepted.SeqNumberReserved : '1'
                    })
        self.ItchLoggedIn = True
        return True

        
    def _OnLOGON(self, data):
        # We expect all fields frrom itchprotocol.MsgLoginRequest
        self.printMsg(MsgLoginRequest, data, trailer='')
        return self.SendLogonResponse(data)

    def SendLogoutResponse(self, data):
        # Function should be overwritten if needed
        # Maybe disconnect?
        # We also may send "End Of Session" msg
        # self.SendEndOfSession(lport, rport)
        self.SendEndOfSession()
        self.ItchLoggedIn = False
        return True

    def _OnLOGOUT(self, data):
        # We expect all fields frrom itchprotocol.MsgLogoutRequest
        self.printMsg(MsgLogoutRequest, data, trailer='')
        return self.SendLogoutResponse(data)

    def SendHeartbeat(self):
        self.SendAsHotspotVenue( {MsgServerHeartBeat.MsgType : 'H' } )
        return True 

    def _OnHEARTBEAT(self, data):
        self.printMsg(MsgClientHeartBeat, data, trailer='')
        # Let's just answer with our heartbeat
        return self.SendHeartbeat()

    def SendMarketSnapshot(self, ccy):
        # Function should be overwritten if needed
        return True

    def _OnMARKET_SNAPSHOT_REQUEST(self, data):
        self.printMsg(MsgMarketSnapshotRequest, data)
        return self.SendMarketSnapshot(data[MsgMarketSnapshotRequest.CurrPair])

    def SendTickerData(self, ccy):
        # Function should be overwritten if needed
        return True

    def _OnTICKER_SUBS_REQUEST(self, data):
        self.printMsg(MsgTickerSubsRequest, data)
        return self.SendTickerData(data[MsgTickerSubsRequest.CurrPair])

    def AdoptUnsubscribeTicker(self, ccy):
        # Function should be overwritten if needed
        return True

    def _OnTICKER_UNSUBS_REQUEST(self, data):
        self.printMsg(MsgTickerUnsubsRequest, data)
        return self.AdoptUnsubscribeTicker(data[MsgTickerUnsubsRequest.CurrPair])

    def SendMDData(self, ccy):
        # Function should be overwritten if needed
        return True

    def _OnMD_SUBS_REQUEST(self, data):
        self.printMsg(MsgMDSubsRequest, data)
        return self.SendMDData(data[MsgMDSubsRequest.CurrPair])

    def AdoptUnsubscribeMD(self, ccy):
        # Function should be overwritten if needed
        return True

    def _OnMD_UNSUBS_REQUEST(self, data):
        self.printMsg(MsgMDUnsubsRequest, data)
        return self.AdoptUnsubscribeMD(data[MsgMDUnsubsRequest.CurrPair])

    def SendInstrumentDirectory(self):
        # Function should be overwritten if needed
        return True

    def _OnINSTR_DIRECTORY_REQUEST(self, data):
        self.printMsg(MsgInstDirRequest, data)
        return slef.SendInstrumentDirectory()

def SettleTS():
    t = datetime.date.today() - datetime.date(1970,1,1)
    return t.days * 86400000 

class ItchVenueClient(ClientBaseCmdProcessor, ItchVenueClientDataProc): #, VSFixVenueClientProcessorHelper):
    OnDataDescriptor = {
    'L':ItchVenueClientDataProc._OnLOGON,
    'O':ItchVenueClientDataProc._OnLOGOUT,
    'R':ItchVenueClientDataProc._OnHEARTBEAT,
    'M':ItchVenueClientDataProc._OnMARKET_SNAPSHOT_REQUEST,
    'T':ItchVenueClientDataProc._OnTICKER_SUBS_REQUEST,
    'U':ItchVenueClientDataProc._OnTICKER_UNSUBS_REQUEST,
    'A':ItchVenueClientDataProc._OnMD_SUBS_REQUEST,
    'B':ItchVenueClientDataProc._OnMD_UNSUBS_REQUEST,
    'I':ItchVenueClientDataProc._OnINSTR_DIRECTORY_REQUEST,
    }
    def __init__(self, args):
        if not hasattr(self, 'name'):
            self.name = "Itch Client"
        super(ItchVenueClient, self).__init__(args)

        self.expected_user_name = args[ItchClientTraits.CREDENTIALS_NAME][0]
        self.expected_user_pass = args[ItchClientTraits.CREDENTIALS_NAME][1]

        # Below controls behavior of based classes
        self.to = 1 # default 5. Timeout to expect any message from Feed (through venue).
        # self.debug = True

    def OnItchMsgFromVenue(self, tup_lport_rport, data):
        (lport, rport) = tup_lport_rport
        try:
            msgtype = data[MESSAGE_TYPE]
        except KeyError as e:
            s = "Fail extract msgtype"
            for mk, mv in kwargs.items():
                s = s + ", {}:{}".format(mk,mv)
            self.Log(s)
            raise
           
        if not (lport, rport) in self.conn:
            raise Exception('Data on unrecorded connection ({}), ({})'.format(lport, rport))

        try:
            connName = self.conn[(lport, rport)]
        except KeyError as e:
            raise Exception('Data on unrecorded connection ({}), ({})'.format(lport, rport))

        try:
            desc = self.rconn[connName]
        except KeyError as e:
            if msgtype == 'L': # Yeah.... Logon request... just record it
                connName = 'md'
                self.conn[(lport, rport)] = connName
                self.rconn[connName]['ports'] = (lport, rport)
                desc = self.rconn[connName]
            else:
                raise Exception('Connection name {n} is not recorded'.format(n=connName))

        return self.OnDataDescriptor[msgtype](self, data)

    def OnMsgFromVenue(self, tup_lport_rport, data):
        (lport, rport) = tup_lport_rport
        return self.OnItchMsgFromVenue((lport, rport), data)
        #if type(data) is dict:
        #    return self.OnItchMsgFromVenue((lport, rport), data)
        #else:
        #    return self.OnFixMsgFromVenue((lport, rport), data) # should be part of vsfixvenue.py

    def SendAsHotspotVenue(self, data):
        if type(data) is dict:
            self.SendDataToServerFromVenue( 
                        {
                            'lport':self.rconn['md']['ports'][0],
                            'rport':self.rconn['md']['ports'][1],
                            VenueBaseCmdTraits.DATA_FIELD_NAME: data
                        }
                    )
        else: # We won't check though assume it is list or tuple od dictionaries to make message containing 2 proto messages
            self.SendDataToServerFromVenue( 
                        {
                            'lport':self.rconn['md']['ports'][0],
                            'rport':self.rconn['md']['ports'][1],
                            VenueBaseCmdTraits.MULTIDATA_FIELD_NAME: data
                        }
                    )

    def SendErrorAsHotspotVenue(self, errTxt):
        self.SendDataToServerFromVenue( 
                        {
                            'lport':self.rconn['md']['ports'][0],
                            'rport':self.rconn['md']['ports'][1],
                            VenueBaseCmdTraits.DATA_FIELD_NAME: {
                                        MsgErrorNotification.MsgType : 'E',
                                        MsgErrorNotification.ErrExplanation : errTxt
                                    }
                        }
                    )

    def SendCloseItchSession(self):
        self.SendCloseSocket(self.rconn['md']['ports'][0], self.rconn['md']['ports'][1])

    def ItchConnectionBroken(self, tup_lport_rport):
        (lport, rport) = tup_lport_rport
        self.Log('Itch connection (ports <{},{}>) is broken'.format(lport, rport))
        
    def OnBrokenConnection(self, tup_lport_rport):
        (lport, rport) = tup_lport_rport
        self.ItchLoggedIn = False
        self.ItchConnectionBroken( (lport, rport) )
        #if self.conn[(lport, rport)] == "md":
        #    self.ItchLoggedIn = False
        #    self.ItchConnectionBroken( (lport, rport) )
        #else:
        #    self.OFFixLoggedIn = False
        #    self.OFFixConnectionBroken( (lport, rport) )

def test():
    venue = ItchVenue()
    cl = ItchVenueClient({ ItchClientTraits.CREDENTIALS_NAME:('1','2'), ClientBaseCmdTraits.PORTS_SET_NAME:['36912']})

if __name__ == "__main__":
    test()
