#!/usr/bin/python
from Venues.Base.Engine.engine import VenueBaseCmdProcessor, ClientBaseCmdProcessor, ClientBaseCmdTraits, VenueBaseCmdTraits
from mffixprotocol import MFFixProtocol
import datetime

class MFFixVenueTraits(object):
    MFFIX_VENUE_CMD_DOMAIN="MFFIX_CMDS"
    def __init__(self):
        pass

class MFFixVenue(VenueBaseCmdProcessor):
    def __init__(self):
        # self.name must be initialized before parent constructor called
        self.name = "MFFIX Venue"
        super(MFFixVenue, self).__init__()

    def AboutToStartServer(self):
        # This is default protocol
        self.protocol = MFFixProtocol(self) # self as a logger
 
class MFFixVenueClientTraits(object):
    USERIDS_TUPLE_NAME    = "UserIDs"
    CONN_DESCRIPTOR_NAME  = "ConnDesc"

# We can use below helper part as component to multi protocol clients
class MFFixVenueClientProcessorHelper(object):
    def __init__(self, args):
        if MFFixVenueClientTraits.USERIDS_TUPLE_NAME in args:
            uids = args[MFFixVenueClientTraits.USERIDS_TUPLE_NAME]
            self.SetUserIDs(uids[0], uids[1]) # from Venues/Base/Engine/engine.py
            self.MDFixLoggedIn = False
            self.OFFixLoggedIn = False
        else:
            # Then MUST be here
            for (name, userid, servingClass) in args[MFFixVenueClientTraits.CONN_DESCRIPTOR_NAME]:
                self.AddRConnDescriptor(name, userid, servingClass) # from Venues/Base/Engine/engine.py

    def GetTag64Value(self):
        dt = datetime.datetime.utcnow() + datetime.timedelta(3)
        dt = dt.date()
        if dt.weekday() == 5:
            dt = dt + datetime.timedelta(2)
        elif dt.weekday() == 6:
            dt = dt + datetime.timedelta(1)
        return  dt.strftime('%Y%m%d')

    def GetTag52Value(self):
        dt = datetime.datetime.utcnow()
        us = int(round(float(dt.microsecond)/1000))
        if us < 10:
            us = '00{us}'.format(us=us)
        elif us < 100:
            us = '0{us}'.format(us=us)
        return dt.strftime('%Y%m%d-%H:%M:%S.{us}'.format(us=us))

    def BuildCommonPart(self, msgType):
        return [
                ('35', msgType),
                ('34',   'Will be overwritten'),
                ('49',   'Will be overwritten'),
                ('56',   'Will be overwritten'),
                ('52',   'Will be overwritten'),
            ]

    def ExtractToFrom(self, data):
        To = ''
        From = ''
        for t,v in data:
            if t == '49':
                From = v
            elif t == '56':
                To = v
        return (To, From)

    def UpdateSeqNumber(self, data, connDesc):
        data[:] = [ (t,v) if t != '34' else (t,str(connDesc['seqN'])) for t,v in data]
        connDesc['seqN'] = connDesc['seqN'] + 1

    def SendFixDataToServerAsVenue(self, d):
        try:
            connName = self.conn[(d['lport'], d['rport'])]
        except KeyError:
            self.Log("SendFixDataToServerAsVenue: Unrecognized port pair")
            return
        t52v = self.GetTag52Value()
        d[VenueBaseCmdTraits.DATA_FIELD_NAME] = [(t,t52v) if t == '52' else 
            (t,self.rconn[connName]['cFrom']) if t == '49' else
            (t,self.rconn[connName]['cTo']) if t=='56' else (t,v) for t,v in d[VenueBaseCmdTraits.DATA_FIELD_NAME]]
        self.UpdateSeqNumber(d[VenueBaseCmdTraits.DATA_FIELD_NAME], self.rconn[connName])
        d[VenueBaseCmdTraits.DATA_FIELD_NAME] = [('8',self.rconn[connName]['proto']),
                   ('9',str(len('|'.join(['{k}={v}'.format(k=k,v=v) for k,v in d[VenueBaseCmdTraits.DATA_FIELD_NAME]])) + 1))] + d[VenueBaseCmdTraits.DATA_FIELD_NAME]
        self.SendDataToServerFromVenue(d)
        
    def UpdateLogonAnswer(self, answer, data):
        return True

    def NoticeMDLoggedOn(self, (lport, rport)):
        self.Log("NoticeMDLoggedOn on {l}-{r}".format(l=lport, r=rport))

    def NoticeOFLoggedOn(self, (lport, rport)):
        self.Log("NoticeOFLoggedOn on {l}-{r}".format(l=lport, r=rport))

    def NoticeLoggedOn(self, connName, (lport, rport)):
        self.Log("NoticeLoggedOn on connection {c} {l}-{r}".format(c=connName, l=lport, r=rport))

    def ConfirmAuth(self, connName, lport):
        if not type(self.exchports[0]) is dict:
            return True
        for pdesc in self.exchports:
            if pdesc[VenueBaseCmdTraits.EXCH_PORT_CONN_NAME] == connName:
                if pdesc[VenueBaseCmdTraits.EXCH_PORT_DESC_KEY_NAME] == str(lport):
                    return True
                return False
        else:
            self.Log("UNEXPECTED! Didn't find connection name {n} in ports' list".format(n=connName))

    def OnFixMsgFromVenue(self, (lport, rport), data):
        self.Log("Received FIX message [{m}]".format(m='|'.join(["{k}={v}".format(k=k,v=v) for k,v in data])))
        msgType = data[2][1]
        if msgType == 'A':
            isMD = True
            To,From = self.ExtractToFrom(data[2:-1])
            if not To or not From:
                self.Log("Login msg [{m}] doesn't contain tag 49. Requesting to close connection".format(
                                                    m='|'.join(["{k}={v}".format(k=k,v=v) for k,v in data])))
                self.SendCloseSocket(lport, rport)
                return
            if self.MdOfMode:
                if self.rconn['md']['userid'] ==  From:
                    self.rconn['md']['ports'] = (lport, rport)
                    self.rconn['md']['seqN']  = 1
                    self.rconn['md']['proto'] = data[0][1]
                    self.rconn['md']['cTo']   = From
                    self.rconn['md']['cFrom'] = To
                    self.conn[(lport, rport)] = 'md'
                    self.rconn['md']['loggedin'] = True
                elif self.rconn['of']['userid'] ==  From:
                    self.rconn['of']['ports'] = (lport, rport)
                    self.rconn['of']['seqN']  = 1
                    self.rconn['of']['proto'] = data[0][1]
                    self.rconn['of']['cTo']   = From
                    self.rconn['of']['cFrom'] = To
                    self.rconn['of']['loggedin'] = True
                    self.conn[(lport, rport)] = 'of'
                    isMD = False
                else:
                    self.Log('Unexpected userid ({u}, expected {md}, {of}). Requesting to close connection'.format(
                                                            u=From,
                                                            md=self.rconn['md']['userid'],
                                                            of=self.rconn['of']['userid']
                                                            ))
                    self.SendCloseSocket(lport, rport)
                    return
            else:
                for dname, d in self.rconn.items():
                    if d['userid'] == From:
                        if self.ConfirmAuth(dname, lport):
                            d['ports'] = (lport, rport)
                            d['seqN']  = 1
                            d['proto'] = data[0][1]
                            d['cTo']   = From
                            d['cFrom'] = To
                            d['loggedin'] = True
                            self.conn[(lport, rport)] = dname
                            break;
                else:
                    self.Log('Unexpected userid ({u}, expected are: {e}'.format(u=From,
                        e=','.join(["{n}:{uid}".format(n=n,uid=d['userid']) for n,d in self.rconn.items()])))
                    self.SendCloseSocket(lport, rport)
                    return
                        
            answer={'lport':lport,'rport':rport}
            rv = data[2:-1]
            answer[VenueBaseCmdTraits.DATA_FIELD_NAME] = rv
            if self.UpdateLogonAnswer(answer, rv):
                self.SendFixDataToServerAsVenue(answer)
                if self.MdOfMode:
                    if isMD:
                        self.MDFixLoggedIn = True
                        self.NoticeMDLoggedOn((lport, rport))
                    else:
                        self.OFFixLoggedIn = True
                        self.NoticeOFLoggedOn((lport, rport))
                else:
                    self.NoticeLoggedOn(self.conn[(lport, rport)], (lport, rport))
        elif msgType == '0':
            answer={'lport':lport,'rport':rport}
            rv = data[2:-1]
            answer[VenueBaseCmdTraits.DATA_FIELD_NAME] = rv
            self.SendFixDataToServerAsVenue(answer)
        elif msgType == MFFixProtocol.CUSTOM_MSG_TYPE:
            self.Log('Got ErrorMsg from venue')
        else:
            connName = self.conn[(lport, rport)]
            if self.MdOfMode:
                if connName == 'md':
                    self.OnMarketData((lport, rport), data[2:-1])
                elif connName == 'of':
                    self.OnTradeFlowData((lport, rport), data[2:-1])
                else:
                    # We probably can't get here since it will fire exception KeyError on connName retrieval
                    # raise Exception("Unrecognized source (no md/of) msg [{m}]".format(m='|'.join(["{k}={v}".format(k=k,v=v) for k,v in data])) )
                    self.Log("Unrecognized source (no md/of) msg [{m}]. Requesting to close connection".format(m='|'.join(["{k}={v}".format(k=k,v=v) for k,v in data])) )
                    self.SendCloseSocket(lport, rport)
                    return
            else:
                desc = self.rconn[connName]
                self.OnFIXData(connName, desc, data[2:-1])

    def OnFIXData(self, connName, desc, data):
        lport, rport = desc['ports']
        self.Log(
          "[{c}][{l}-{r}]".format(c=connName,l=lport,r=rport) +
          "Please overwrite me. I've got base OnFIXData call!!!!!!")

    def OnMarketData(self, (lport, rport), data):
        self.Log("Please overwrite me. I've got MFFixVenueClientProcessorHelper.OnMarketData call!!!!!!")

    def OnTradeFlowData(self, (lport, rport), data):
        self.Log("Please overwrite me. I've got MFFixVenueClientProcessorHelper.OnTradeFlowData call!!!!!!")

    def MDFixConnectionBroken(self, (lport, rport) ):
        self.Log('FIX (MD) connection (ports <{},{}>) is broken'.format(lport, rport))

    def OFFixConnectionBroken(self, (lport, rport) ):
        self.Log('FIX (trade) connection (ports <{},{}>) is broken'.format(lport, rport))

    def TheFixConnectionBroken(self, connName, (lport, rport) ):
        self.Log('FIX ({}) connection (ports <{},{}>) is broken'.format(connName, lport, rport))

    def OnBrokenConnection(self, (lport, rport) ):
        connName = self.conn[(lport, rport)]
        if self.MdOfMode:
            if connName == "md":
                self.MDFixLoggedIn = False
                self.MDFixConnectionBroken( (lport, rport) )
            else:
                self.OFFixLoggedIn = False
                self.OFFixConnectionBroken( (lport, rport) )
        else:
            self.TheFixConnectionBroken(connName, (lport, rport) )

class MFFixVenueClient(ClientBaseCmdProcessor, MFFixVenueClientProcessorHelper):
    def __init__(self, args):
        # self.name should be set before it in derived test client
        if not hasattr(self, 'name'):
            self.name = "MFFix Client"
        super(MFFixVenueClient, self).__init__(args)
       
    def OnMsgFromVenue(self, (lport, rport), data):
        self.OnFixMsgFromVenue((lport, rport), data)

def test():
    venue = MFFixVenue()
    cl=MFFixVenueClient({MFFixVenueClientTraits.USERIDS_TUPLE_NAME : ('marketdata', 'orderflow'),
             ClientBaseCmdTraits.PORTS_SET_NAME:['36912', '36914']})
    cl=MFFixVenueClient({MFFixVenueClientTraits.CONN_DESCRIPTOR_NAME: (('md','marketdata'), ('of','orderflow')),
             ClientBaseCmdTraits.PORTS_SET_NAME:['36912', '36914']})
if __name__ == "__main__":
    test()
