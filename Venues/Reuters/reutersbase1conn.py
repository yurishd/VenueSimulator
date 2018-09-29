#!/usr/bin/python
from Venues.Base.Engine.engine import ClientBaseCmdTraits, VenueBaseCmdTraits
from Venues.Base.mffix.mffixvenue import MFFixVenue, MFFixVenueClient, MFFixVenueClientTraits
import datetime

class TFVCTraits(object):
    STATE_INIT              = 'STATE_INIT'
    STATE_LOGGEDIN          = 'STATE_LOGGEDIN'
    STATE_MARKETCLOSED_SENT = 'STATE_MARKETCLOSED_SENT'
    STATE_SOTW_SENT         = 'STATE_SOTW_SENT'
    STATE_MARKETOPENED_SENT = 'STATE_MARKETOPENED_SENT'
    STATE_READY_TO_TEST     = 'STATE_READY_TO_TEST'

class BaseFixVenueClientReuters1Conn (MFFixVenueClient):
    def __init__(self, args, sessionExpectedPswrd, userReqExpectedPswrd):
        if not hasattr(self, 'name'): # Derived may set it up
            self.name = "Test Fix Client" # Must be before super
        super(BaseFixVenueClientReuters1Conn, self).__init__(args)
        self.subCTo   = ''
        self.subCFrom = ''
        self.TFVCState  = TFVCTraits.STATE_INIT
        self.heartBeatHappened = False
        self.sessionExpectedPswrd = sessionExpectedPswrd
        self.userReqExpectedPswrd = userReqExpectedPswrd

    def DoTest(self):
        self.Log('Inside base DoTest. Please overwrite')

    def BuildCommonPart(self, msgType):
        return [
                ('35', msgType),
                ('50',   'Will be overwritten'),
                ('57',   'Will be overwritten'),
                ('34',   'Will be overwritten'),
                ('1128', '9'), # AppVerID, FIX5.0SP2
                ('49',   'Will be overwritten'),
                ('56',   'Will be overwritten'),
                ('52',   'Will be overwritten')
            ]


    def BuildUserNotificationBase(self):
        return self.BuildCommonPart('CB') + [
               ('926',  '6') # UserStatus 'Other'
            ]

    def SendMarketClosed(self):
        self.SendTestTradeData( {'lport' : self.rconn['of']['ports'][0], 'rport' : self.rconn['of']['ports'][1]},
                            self.BuildUserNotificationBase() + [('58', 'MTM:0')])
        self.TFVCState = TFVCTraits.STATE_MARKETCLOSED_SENT

    def SendSOTW(self):
        self.SendTestTradeData( {'lport' : self.rconn['of']['ports'][0], 'rport' : self.rconn['of']['ports'][1]},
                            self.BuildUserNotificationBase() + [('58', 'SOTW:1')])
        self.TFVCState = TFVCTraits.STATE_SOTW_SENT

    def SendMarketOpened(self):
        self.SendTestTradeData( {'lport' : self.rconn['of']['ports'][0], 'rport' : self.rconn['of']['ports'][1]},
                            self.BuildUserNotificationBase() + [('58', 'MTM:1')])
        self.TFVCState = TFVCTraits.STATE_MARKETOPENED_SENT

    def OnDataDone(self):
        self.Log("OnDataDone. State is [{s}]".format(s=self.TFVCState))
        if self.TFVCState == TFVCTraits.STATE_READY_TO_TEST:
            self.DoTest()
        elif self.TFVCState == TFVCTraits.STATE_LOGGEDIN:
            # self.SendMarketClosed()
            self.SendMarketOpened()
        elif self.TFVCState == TFVCTraits.STATE_MARKETCLOSED_SENT:
            self.SendSOTW()
            self.SOWT_TS = datetime.datetime.now()
        elif self.TFVCState == TFVCTraits.STATE_SOTW_SENT:
            if (datetime.datetime.now() - self.SOWT_TS).seconds >= 60:
                self.SendMarketOpened()
        elif self.TFVCState == TFVCTraits.STATE_MARKETOPENED_SENT:
            self.TFVCState = TFVCTraits.STATE_READY_TO_TEST
        else:
            self.Log("OnDataDone. State is [{s}]".format(s=self.TFVCState))
        return True

    def ActionOnSessionLogonFail(self, passwrd):
        raise Exception('Failed to authenticate Session. Expected <{e}>, received <{r}>)'.format(e=self.sessionExpectedPswrd,r=passwrd))

    def UpdateLogonAnswer(self, answer, data):
        d = dict(data)
        if self.sessionExpectedPswrd != d['554']:
            self.ActionOnSessionLogonFail(d['554'])
            return False
        else:
            data.append( ('58', '1234AuthToken') )
            return True

    def OnMarketData(self, (lport, rport), data):
        self.Log("Received MD message [{m}]".format(m='|'.join(["{k}={v}".format(k=k,v=v) for k,v in data])))

    def SendTestTradeData(self, answer, rv):
        # Setup subComIDs (tags 50 and 57). Tags 49 and 56 will be set by mffix client
        rv[:] = [(t, self.subCFrom) if t == '50' else (t,self.subCTo) if t == '57' else (t,v) for t,v in rv]
        answer[VenueBaseCmdTraits.DATA_FIELD_NAME] = rv
        self.SendFixDataToWhsprrFromVenue(answer)

    def OnTradeHeartBeat(self, answer, rv):
        # Nothing to do... just ping back
        self.SendTestTradeData(answer, rv)

    def ActionOnUserReqFail(self, answer, uname, psswrd, ureqid):
        raise Exception('Failed to authenticate User Request. Expected <{e}>, received <{r}>)'.format(e=self.userReqExpectedPswrd,r=psswrd))

    def OnUserRequest(self, answer, rv):
        for t,v in rv:
            if t == '50':
                self.subCTo = v
            elif t == '57':
                self.subCFrom = v
            elif t == '553': # UserName
                uname = v
            elif t == '554': # Password
                psswrd = v
            elif t == '923': # UserReqID
                ureqid = v
        if psswrd != self.userReqExpectedPswrd:
            self.ActionOnUserReqFail(answer, uname, psswrd, ureqid)
        else:
            data = [
                ('35',   'BF'), # UserResponse
                ('50',   'Will be overwritten'),
                ('57',   'Will be overwritten'),
                ('34',   'Will be overwritten'),
                ('1128', '9'), # AppVerID, FIX5.0SP2
                ('49',   'Will be overwritten'),
                ('56',   'Will be overwritten'),
                ('52',   'Will be overwritten'),
                ('923',  ureqid),
                ('553',  uname),
                ('926',  '1'), # LoggedIn
                ('927',  'YSH UserStatusText field')
            ]
            self.SendTestTradeData(answer, data)
            self.TFVCState = TFVCTraits.STATE_LOGGEDIN

    def OnTestTradeFlowData( self, ports, data ):
        (lport, rport) = ports
        self.Log("Received OF message [{m}]".format(m='|'.join(["{k}={v}".format(k=k,v=v) for k,v in data])))

    def OnTradeFlowData(self, (lport, rport), data):
        answer={'lport':lport,'rport':rport}
        rv = data[:] # Deep copy
        msgType = rv[0][1]
        if msgType == '0': # Heartbeat
            self.heartBeatHappened = True
            self.OnTradeHeartBeat(answer, rv)
        elif msgType == 'BE': # UserRequest
            if not self.heartBeatHappened:
                raise Exception("Fail Reuters's logon protocol")
            self.OnUserRequest(answer, rv)
        elif msgType == '5': # logout 
            # Just try to send back the same what received.
            # Engine "beneath" should figure out
            self.SendTestTradeData(answer, rv)
        else:
            self.OnTestTradeFlowData((lport, rport), rv)

def main():
    args = {
        ClientBaseCmdTraits.PORTS_SET_NAME        : ['36914'],
        MFFixVenueClientTraits.USERIDS_TUPLE_NAME : ('marketdataNotInUse', 'YSH-TEST-OF')
    }
    sessionExpectedPswrd = 'YSHSessPass'
    userReqExpectedPswrd = 'YSHUserReqPass'
    venue = MFFixVenue()
    cl = BaseFixVenueClientReuters1Conn(args, sessionExpectedPswrd, userReqExpectedPswrd)
    cl.StartVenueProcess(venue)
    cl.StartVenueService()
    cl.PumpOutData()
    cl.StopVenueService()
    cl.StopVenueProcess()
    cl.PumpOutData()
    print "After cl.StopVenueProcess"


if __name__ == "__main__":
    main()

