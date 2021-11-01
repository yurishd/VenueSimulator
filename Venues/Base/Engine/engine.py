#!/usr/bin/python
import socket
import select
from multiprocessing import Process
import json
import struct
import importlib
#import pdb
import sys
import time

class CmdNetCommunicator(object):
    """
    CmdNetCommunicator is the engine that listens sockets and calls callbacks
    There are data sockets and one command socket
    """
    def __init__(self, *args, **kwargs):
        super(CmdNetCommunicator, self).__init__(*args, **kwargs)
        self.to = 5 # TimeOut being in "select"
        self.inSelect = False # this is not accurate though just as helper (for multithreading) anyway....
        self.cmdSocket = -1
        self.potentialReaders=[]
        self.debug = False

    def _SendOnCmdSocket(self, msg):
        self.cmdSocket.send(struct.pack('!I', len(msg)))
        self.cmdSocket.send(msg)

    def SendCmd(self, domain, cmd, param=''):
        self._SendOnCmdSocket( json.dumps( {'D':domain, 'C':cmd, 'P':param} ) )

    def processTO(self):
        # just do nothing
        self.Log("In timeout")
        return True

    def SetCmdSocket(self, s):
        if self.cmdSocket != -1:
            if not RemoveSocket(self.cmdSocket):
                print ("{}, fail to set new cmd socket".format(self.name))
                return False
            self.cmdSocket.close()
        self.cmdSocket = s
        self.potentialReaders.append(s)
        return True

    def AddSocket(self, s):
        if self.inSelect:
            print ("{}, fail to add socket since in select call".format(self.name))
            return False
        self.potentialReaders.append(s)
        return True

    def RemoveSocket(self, sToRemove):
        if self.inSelect:
            print ("{}, fail to remove socket since in select call".format(self.name))
            return False
        tmp = []
        for s in self.potentialReaders:
            if s != sToRemove:
                tmp.append(s)
        self.potentialReaders = tmp
        return True

    def OnCmdDone(self, rvFromLastCmd):
        return rvFromLastCmd

    def ProcessCmd(self, msg):
        #self.Log("Got from cmd socket [{}]".format(msg))
        #cmds must be established by derived class
        rv = self.cmds(msg)
        return self.OnCmdDone(rv)  # Allow derived to do anything on the event "Cmd Done"

    def OnCmdSocketDisconnect(self):
        #here just do nothing
        pass

    def Log(self, msg):
        self.LogInternal("{}: {}".format(self.name, msg))

    def loop(self):
        toRead = 4
        doLen = True
        buff = ""
        lenbuff = ""
        doLoop=True
        while doLoop:
            self.inSelect = True
            readyToRead, readyToWrite, inError = select.select(
                self.potentialReaders,
                [],
                self.potentialReaders,
                self.to)
            self.inSelect = False
            if not readyToRead:
                doLoop = self.processTO()
                continue
            for s in readyToRead:
                if s == self.cmdSocket:
                    data = s.recv(toRead)
                    if 0 == len(data):
                        # Well, peer closed socket. Get out
                        print ("{}, got disconnect command socket".format(self.name))
                        doLoop = False
                        self.OnCmdSocketDisconnect()
                        break;
                    toRead = toRead - len(data)
                    if doLen:
                        lenbuff = lenbuff + data
                        if 0 == toRead:
                            (toRead, ) = struct.unpack('!I', lenbuff)
                            lenbuff = ""
                            doLen = False
                    else:
                        buff = buff + data
                        if 0 == toRead:
                            toRead = 4
                            #cmds must be established by derived class
                            doLoop = self.ProcessCmd(buff)
                            buff = ""
                            doLen = True
                else:
                    doLoop = self.ProcessData(s)
        self.Log( "Leaving main loop" );

#look in stack overflow
def byteify(input):
    if isinstance(input, dict):
        return {byteify(key): byteify(value)
                for key, value in input.items()}
    elif isinstance(input, list):
        return [byteify(element) for element in input]
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    else:
        return input

class CmdExecutor(object):
    def __init__(self, tester):
        super(CmdExecutor, self).__init__()
        self.tester = tester
        self.funcs = {}

    def RegisterCmd(self, cmddomain, cmdname, cmdcallback):
        logdomain = 'existing'
        if not cmddomain in self.funcs.keys():
            self.funcs[cmddomain] = {}
            logdomain = 'new'
        domain = self.funcs[cmddomain]
        logcmd = 'new'
        if cmdname in domain.keys():
            logcmd = 'overwriting'
        domain[cmdname] = cmdcallback
        if not cmddomain in self.funcs:
            self.tester.Log("RegisterCmd, Very BAD. no domain [{}]\nExiting".format(cmddomain))
            exit (1)
        if not cmdname in self.funcs[cmddomain]:
            self.tester.Log("RegisterCmd, Very BAD. no cmd [{}] in domain [{}]\nExiting".format(cmdname, cmddomain))
            exit (1)
        self.tester.Log("RegisterCmd, {} cmd [{}] in {} domain [{}] is added".format(logcmd, cmdname, logdomain, cmddomain))

    def __call__(self, cmdbuf):
        cmd = byteify(json.loads(cmdbuf))
        try:
            domain = self.funcs[cmd['D']]
        except KeyError as e:
            self.tester.Log("Error: Either missing key 'D' or such domain is not registered\n{b}\nCommand skipped...".format(b=cmdbuf))
            return None
        try:
            f = domain[cmd['C']]
        except KeyError as e:
            self.tester.Log("Error: Missing key 'C' in cmd\n{b}\nCommand skipped...".format(b=cmdbuf))
            return None
        if 'P' in cmd:
            if type (cmd['P']) == type(dict()):
                return f(self.tester, **cmd['P'])
            elif type (cmd['P']) == type(list()):
                return f(self.tester, *cmd['P'])
            else:
                return f(self.tester, cmd['P'])
        else:
            return f(self.tester)

class VenueBaseCmdTraits(object):
    DOMAIN_NAME                = 'VENUE_BASE_CMD_SET'
    EXCH_PORT_DESC_KEY_NAME    = 'port'
    EXCH_PORT_PROTO            = 'proto'
    EXCH_PORT_PROTO_MODULE     = 'modulename'
    EXCH_PORT_PROTO_CLASS_NAME = 'protoclassname'
    EXCH_PORT_PROTO_CLASS_ARGS = 'protoclassargs'
    EXCH_PORT_CONN_NAME        = 'connectionname'
    EXCH_PORTS_KEY_NAME        = 'exchports'
    DATA_FIELD_NAME            = 'data'
    MULTIDATA_FIELD_NAME       = 'multidata'
    NOTICE_KEY                 = 'event'
    NOTICE_VALUE_LISTENNING    = 'L'
    NOTICE_VALUE_CONNECTED     = 'C'
    NOTICE_VALUE_DISCONNECTED  = 'D'
    NOTICE_VALUE_STARTED       = 'S'
    NOTICE_VALUE_FINISHED      = 'F'

    def init(self):
        pass

class ClientBaseCmdTraits(object):
    DOMAIN_NAME    = "CLIENT_BASE_CMD_SET"
    PORTS_SET_NAME = 'PortsToListen'

class VenueBaseCmdProcessor(CmdNetCommunicator):
    def __init__(self):
        super(VenueBaseCmdProcessor, self).__init__()
        self.expectHeartbeat = False
        self.hbstr = '{ob}"D":"{D}","C":"H"{cb}'.format(ob='{', D=VenueBaseCmdTraits.DOMAIN_NAME, cb='}')
        self.exchsocks = set(); # Sockets that are listening for FeedHandker to connect
        self.datasocks = set(); # Sockets that are established connection (md/of or both)
        self.dsocksData = {}    # Data per data socket to be parsed
        self.started = False
        self.logQue = []
        self.to = 10
        #########
        #   CMDs
        #########
        self.cmds = CmdExecutor(self);
        self.cmds.RegisterCmd(VenueBaseCmdTraits.DOMAIN_NAME, 'H',     VenueBaseCmdProcessor.Heartbeat)
        self.cmds.RegisterCmd(VenueBaseCmdTraits.DOMAIN_NAME, 'START', VenueBaseCmdProcessor.DoStart)
        self.cmds.RegisterCmd(VenueBaseCmdTraits.DOMAIN_NAME, 'STOP',  VenueBaseCmdProcessor.DoStop)
        self.cmds.RegisterCmd(VenueBaseCmdTraits.DOMAIN_NAME, 'KILL',  VenueBaseCmdProcessor.DoKill)
        self.cmds.RegisterCmd(VenueBaseCmdTraits.DOMAIN_NAME, 'D',     VenueBaseCmdProcessor.OnDataToSend)
        self.cmds.RegisterCmd(VenueBaseCmdTraits.DOMAIN_NAME, 'C',     VenueBaseCmdProcessor.OnCloseDataSocket)

    def NoticeNewListenSocket(self, ls, lport):
        self.Log("{n}, Listening on port {p} with proto {t}".format(n=self.name, p=lport, t = self.dsocksData[lport]['proto'].Name()))
        self.SendCmd(ClientBaseCmdTraits.DOMAIN_NAME, 'N',
                        {
                            VenueBaseCmdTraits.NOTICE_KEY : VenueBaseCmdTraits.NOTICE_VALUE_LISTENNING,
                            'lport'                       : lport,
                            'proto'                       : self.dsocksData[lport]['proto'].Name()
                        }
                    )
        return True

    def NoticeNewConnection(self, ls, ds):
        (lhost, lport) = ds.getsockname()
        if not lport in self.dsocksData.keys():
            self.Log("Got new connection on unexpected port {}. Exiting...".format(lport))
            return False
        (rhost, rport) = ds.getpeername()
        self.dsocksData[lport][rport] = {'data':"", 'sock': ds}
        self.SendCmd(ClientBaseCmdTraits.DOMAIN_NAME, 'N',
                        {
                            VenueBaseCmdTraits.NOTICE_KEY : VenueBaseCmdTraits.NOTICE_VALUE_CONNECTED,
                            'lport'                       : lport,
                            'rport'                       : rport
                        }
                    )
        return True

    def NoticeDisconnection(self, ports):
        lport, rport = ports
        self.SendCmd(ClientBaseCmdTraits.DOMAIN_NAME, 'N',
                        {
                            VenueBaseCmdTraits.NOTICE_KEY : VenueBaseCmdTraits.NOTICE_VALUE_DISCONNECTED,
                            'lport'                       : lport,
                            'rport'                       : rport
                        }
                    )
        try:
            sc = self.dsocksData[lport][rport]['sock']
            self.dsocksData[lport].pop(rport, None)
            self.RemoveSocket(sc)
        except KeyError:
            return True
        return True

    def OnData(self, ds):
        data = ds.recv(10240)
        (rhost, rport) = ds.getpeername()
        (lhost, lport) = ds.getsockname()
        if len(data) == 0:
            return self.NoticeDisconnection( (lport, rport) )
        self.dsocksData[lport][rport]['data'] = self.dsocksData[lport][rport]['data'] + data
        protocol = self.dsocksData[lport]['proto']
        bytesLeft = len(self.dsocksData[lport][rport]['data'])
        while bytesLeft > 0:
            # self.Log('In about to parse {n}'.format(n=len(self.dsocksData[lport][rport]['data'])))
            parsedBytes, msgDict = protocol.parse(self.dsocksData[lport][rport]['data'], self.debug)
            if 0 == parsedBytes:
                return True
            if 0 > parsedBytes:
                self.Log("Fail to parse data. Exiting...")
                return False
            bytesLeft = bytesLeft - parsedBytes
            if parsedBytes == len(self.dsocksData[lport][rport]['data']):
                self.dsocksData[lport][rport]['data'] = ""
            else:
                self.dsocksData[lport][rport]['data'] = self.dsocksData[lport][rport]['data'][parsedBytes:]
            d = {'lport':lport,'rport':rport, VenueBaseCmdTraits.DATA_FIELD_NAME:msgDict}
            self.SendCmd(ClientBaseCmdTraits.DOMAIN_NAME,'D', d)
        return True

    def OnCloseDataSocket(self, *args, **kwargs):
        # do not check anything. If exception so to be
        lport = kwargs['lport']
        rport = kwargs['rport']
        try:
            sc = self.dsocksData[lport][rport]['sock']
            self.dsocksData[lport].pop(rport, None)
            self.RemoveSocket(sc)
            sc.shutdown(socket.SHUT_RD)
            sc.close()
            self.SendCmd(ClientBaseCmdTraits.DOMAIN_NAME, 'N',
                        {
                            VenueBaseCmdTraits.NOTICE_KEY : VenueBaseCmdTraits.NOTICE_VALUE_DISCONNECTED,
                            'lport'                       : lport,
                            'rport'                       : rport
                        }
                    )
        except KeyError:
            pass
        return True


    def OnDataToSend(self, *args, **kwargs):
        # do not check anything. If exception so to be
        lport = kwargs['lport']
        rport = kwargs['rport']
        protocol = self.dsocksData[lport]['proto']
        if VenueBaseCmdTraits.DATA_FIELD_NAME in kwargs:
            d = protocol.pack(kwargs[VenueBaseCmdTraits.DATA_FIELD_NAME])
        elif VenueBaseCmdTraits.MULTIDATA_FIELD_NAME in kwargs:
            d = ''.join([protocol.pack(item) for item in kwargs[VenueBaseCmdTraits.MULTIDATA_FIELD_NAME]])
        else:
            raise Exception("Fail to find data to send")
            
        try:
            dataIsBynary = protocol.isDataBynary()
        except AttributeError:
            dataIsBynary = False
        if dataIsBynary:
            self.Log("OnDataToSend: About to send BINARY data count [{c}] to [{l}/{r}]".format(
                                            c=len(d),
                                            l=lport,
                                            r=rport
                                            ))
        else:
            self.Log("OnDataToSend: About to send to {}, {}, data:{}".format(lport, rport,d))

        # remote port at this point could be disconnected so just "don't crush"
        try:
            self.dsocksData[lport][rport]['sock'].sendall(d)
        except KeyError:
            pass
        return True;

    def CreateProtocol(self, portDesc):
        if not VenueBaseCmdTraits.EXCH_PORT_PROTO in portDesc:
            self.Log("Using DEFAULT proto")
            return self.protocol
        # If we are here then we assume we have dict with proper params
        protoDesc = portDesc[VenueBaseCmdTraits.EXCH_PORT_PROTO]
        m = importlib.import_module(protoDesc[VenueBaseCmdTraits.EXCH_PORT_PROTO_MODULE])
        if VenueBaseCmdTraits.EXCH_PORT_PROTO_CLASS_ARGS in protoDesc:
            self.Log("Argument for proto is DETECTED")
            return getattr(m, protoDesc[VenueBaseCmdTraits.EXCH_PORT_PROTO_CLASS_NAME])(self, 
                    **protoDesc[VenueBaseCmdTraits.EXCH_PORT_PROTO_CLASS_ARGS])
        else:
            self.Log("Argument for proto is NOT detected")
            return getattr(m, protoDesc[VenueBaseCmdTraits.EXCH_PORT_PROTO_CLASS_NAME])(self)

    def DoStart(self, *args, **kwargs):
        if not VenueBaseCmdTraits.EXCH_PORTS_KEY_NAME in kwargs:
            raise Exception("Didn't get expected parameter {p}. exiting".format(p = VenueBaseCmdTraits.EXCH_PORTS_KEY_NAME))
        for portDesc in kwargs[VenueBaseCmdTraits.EXCH_PORTS_KEY_NAME]:
            if type(portDesc) is str:
                portDesc = {VenueBaseCmdTraits.EXCH_PORT_DESC_KEY_NAME : portDesc}
            elif not type(portDesc) is dict:
                raise Exception("Port descriptor argument should be either STR or DICT")
            elif not VenueBaseCmdTraits.EXCH_PORT_DESC_KEY_NAME in portDesc:
                raise Exception("Port descriptor argument should contain key '{p}'".format(p=VenueBaseCmdTraits.EXCH_PORT_DESC_KEY_NAME))
                
            try:
                port = int(portDesc[VenueBaseCmdTraits.EXCH_PORT_DESC_KEY_NAME])
            except ValueError:
                self.Log("Listening on port {} - fail to convert to integer. Exiting...".format(
                                                portDesc[VenueBaseCmdTraits.EXCH_PORT_DESC_KEY_NAME]))
                return False
            s = socket.socket()
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            #s.bind((socket.gethostname(), port))
            s.bind(("localhost", port))
            s.setblocking(0)
            s.listen(5)
            self.exchsocks.add(s)
            self.AddSocket(s)
            self.dsocksData[port] = {'proto': self.CreateProtocol(portDesc)}
            self.NoticeNewListenSocket(s, port)
        return True

    def DoStop(self, *args, **kwargs):
        for s in self.datasocks:
            self.RemoveSocket(s)
            s.close()
        for s in self.exchsocks:
            self.RemoveSocket(s)
            s.close()
        self.Log("Stopped")
        return True

    def Heartbeat(self, *args, **kwargs):
        if not self.expectHeartbeat:
            self.send(self.hbstr)
            self.Log("Received hb request")
        else:
            self.expectHeartbeat = False # this is response on H
            self.Log("Received hb response")
        return True

    def DoKill(self, *args, **kwargs):
        # Returning False will cause exit from loop
        self.Log("Going to kill itself")
        return False

    def ProcessData(self, s):
        if s in self.exchsocks:
            d, addr = s.accept()
            d.setblocking(0)
            self.AddSocket(d);
            self.datasocks.add(d)
            return self.NoticeNewConnection(s, d) 
        else:
            return self.OnData(s)

    def StartAsServer(self, sToClose):
        (host, port) = sToClose.getsockname()
        cmdsock = socket.socket()
        cmdsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        cmdsock.connect((host, port))
        cmdsock.setblocking(0)
        self.SetCmdSocket(cmdsock)
        sToClose.close()
        self.AboutToStartServer()
        self.started = True
        self._FlushLogs()
        self._NoticeStarted()
        self.loop()
        self._NoticeFinished()
        # Just let all go through
        time.sleep(2)

    def LogInternal(self, msg):
        if self.started:
            self.SendCmd(ClientBaseCmdTraits.DOMAIN_NAME, 'L', msg);
        else:
            self.logQue.append(msg);

    def _FlushLogs(self):
        if self.started:
            for l in self.logQue:
                self.LogInternal(l)

    def _NoticeStarted(self):
        self.SendCmd(ClientBaseCmdTraits.DOMAIN_NAME, 'N',
                        {
                            VenueBaseCmdTraits.NOTICE_KEY : VenueBaseCmdTraits.NOTICE_VALUE_STARTED
                        }
                    )

    def _NoticeFinished(self):
        self.SendCmd(ClientBaseCmdTraits.DOMAIN_NAME, 'N',
                        {
                            VenueBaseCmdTraits.NOTICE_KEY : VenueBaseCmdTraits.NOTICE_VALUE_FINISHED
                        }
                    )
class ConnDesc(object):
    def __init__(self, name, userid='UNSET', ports=(), seqNRx=0, seqNSnd=0):
        self.name = name;
        self.data={
                'userid'  : userid,
                'ports'   : ports,
                'seqNRx'  : seqNRx,
                'seqNSnd' : seqNSnd,
                'loggedin': False,
                'name'    : name,
                VenueBaseCmdTraits.EXCH_PORT_DESC_KEY_NAME:'',
                'servingClass':None
                };

class ClientBaseCmdProcessor(CmdNetCommunicator):
    def __init__(self, args):
        self.rconn = {}
        # this is (could be "are") port(s) that "venue" will be listening
        # if ClientBaseCmdTraits.PORTS_SET_NAME in args:
        #     self.Log('Setting excahnge ports {n}'.format(n = len(args[ClientBaseCmdTraits.PORTS_SET_NAME])))
        # else:
        #     self.Log('No exchange ports')
        self.exchports = args[ClientBaseCmdTraits.PORTS_SET_NAME] if ClientBaseCmdTraits.PORTS_SET_NAME in args else []
        self.MdOfMode = False
        super(ClientBaseCmdProcessor, self).__init__(args)
        #########
        #   CMDs
        #########
        self.cmds = CmdExecutor(self);
        self.cmds.RegisterCmd(ClientBaseCmdTraits.DOMAIN_NAME, 'N', ClientBaseCmdProcessor.OnNotice)
        self.cmds.RegisterCmd(ClientBaseCmdTraits.DOMAIN_NAME, 'L', ClientBaseCmdProcessor.RemoteLog)
        self.cmds.RegisterCmd(ClientBaseCmdTraits.DOMAIN_NAME, 'D', ClientBaseCmdProcessor.OnDataFromVenue)
        self.timeoutHappened = False
        self.connected = False
        self.ultimatelyGetOutOfLoop = False
        self.conn = {}  # Once client got login info on specific connection, it should set item in this
                        # map with key as tuple (lport, rport) and value as connection's name (f.e. 'md' or 'of').
                        # The self.rconn[ConnName]['userid'] should help with this discover
        self.myState = "init"
        self.InitOnNoticeCallbacks()

    def AddRConnDescriptor(self, name, userid, servingClass):
        scname = "NoServingClass"
        if servingClass:
            scname = servingClass.__name__
        self.Log("Setting up connection with name '{n}', userid '{u} and serviceClass {sc}'".format(
                                                                        n=name,
                                                                        u=userid,
                                                                        sc = scname))
        if not type(self.exchports[0]) is dict:
            raise Exception("AddRConnDescriptor could be used only with FULL port descriptors")
        for pdesc in self.exchports:
            if pdesc[VenueBaseCmdTraits.EXCH_PORT_CONN_NAME] == name:
                # Perfect
                desc = ConnDesc(name, userid)
                self.rconn[desc.name] = desc.data
                self.rconn[desc.name][VenueBaseCmdTraits.EXCH_PORT_DESC_KEY_NAME] = \
                        pdesc[VenueBaseCmdTraits.EXCH_PORT_DESC_KEY_NAME]
                self.rconn[desc.name]['servingClass'] = servingClass
                return
        else:
            raise Exception("AddRConnDescriptor, port description for connection {n} is not found".format(n=name))

    def SetUserIDs(self, mdUserID, ofUserID):
        self.MdOfMode = True
        desc = ConnDesc('md')
        self.rconn[desc.name] = desc.data
        desc = ConnDesc('of')
        self.rconn[desc.name] = desc.data
        if mdUserID:
            self.rconn['md']['userid'] = mdUserID
            self.Log("Setting up MD Connection userid as [{md}]".format(md=mdUserID))
        else:
            if not 'userid' in self.rconn['md']:
                self.rconn['md']['userid'] = 'Not Set Up'
            self.Log("Skipping MD Connection userid. Left as [{md}]".format(md=self.rconn['md']['userid']))
        if ofUserID:
            self.rconn['of']['userid'] = ofUserID
            self.Log("Setting up OF Connection userid as [{of}]".format(of=ofUserID))
        else:
            if not 'userid' in self.rconn['of']:
                self.rconn['of']['userid'] = 'Not Set Up'
            self.Log("Skipping OF Connection userid. Left as [{of}]".format(of=self.rconn['of']['userid']))

    def processTO(self):
        if self.myState == "init":
            self.StartVenueService()
            self.myState = "running pending"
        elif self.myState == "running pending":
            # Nothing is going on
            raise Exception("No notices from server")
        else:
            self.timeoutHappened = True
        return True

    def OnNoticeSTARTED(self, **kwargs):
        self.isVenueStarted = True
        self.Log("Notice, Venue is UP")
        return False # This Notice return False
        
    def OnNoticeFINISHED(self, **kwargs):
        self.isVenueStarted = False
        self.Log("Notice, Venue is DOWN")
        return False # This Notice return False

    def OnStartListening(self, port, cname, proto):
        if cname:
            n = cname
        else:
            n = 'UNKNOWN'
        self.Log('Started listening on port [{p}] for conn [{n}] with proto [{pr}]'.format(
                            p = port,
                            n = n,
                            pr = proto
                            ))
        return True

    def OnNoticeLISTENNING(self, **kwargs):
        try:
            port = str(kwargs['lport'])
            self.myState = "running"
            for cname, desc in self.rconn.items():
                if desc['servingClass']:
                    if desc[VenueBaseCmdTraits.EXCH_PORT_DESC_KEY_NAME] == port:
                        return desc['servingClass'].OnStartListening(self, port, cname, kwargs['proto'])
            else:
                return self.OnStartListening(port, '', kwargs['proto'])
        except KeyError:
            self.Log("_OnNoticeL: Missing expected parameter lport")
            return False

    def OnNewConnection(self, conn_data, connname ):
        lport, rport = conn_data
        if connname:
            name = connname
        else:
            name = 'UNKNOWN'
        self.Log('Got notice for new connection type [{n}] for pair ({l},{r})'.format(
                                                                            n = connname,
                                                                            l = lport,
                                                                            r = rport))
        return True;

    def OnNoticeCONNECTED(self, **kwargs):
        try:
            lport = kwargs['lport']
        except KeyError:
            self.Log("OnNoticeCONNECTED: Missing expected parameter lport")
            return False
        try:
            rport = kwargs['rport']
        except KeyError:
            self.Log("OnNoticeCONNECTED: Missing expected parameter rport")
            return False

        try:
            if (lport, rport) in self.conn:
                raise Exception('Unexpected "Connection Notice" on already recorded pair {},{}'.format(lport, rport))
            self.conn[(lport, rport)] = ""
            slport = str(lport)
            for cname,desc in self.rconn.items():
                if VenueBaseCmdTraits.EXCH_PORT_DESC_KEY_NAME in desc:
                    if desc[VenueBaseCmdTraits.EXCH_PORT_DESC_KEY_NAME] == slport:
                        self.conn[(lport, rport)] = cname
                        break
            # It is possible that self.conn[(lport, rport)] is not set yet
            # This could happen only if there was short ports initialization
            cname = self.conn[(lport, rport)]
            if cname in self.rconn and self.rconn[cname]['servingClass']:
                return self.rconn[cname]['servingClass'].OnNewConnection(self, (lport, rport), cname)
            return self.OnNewConnection( (lport, rport), cname )
        except KeyError:
            self.Log("OnNoticeCONNECTED: Missing conn for port {}?". format(lport))
            return False

    def OnBrokenConnection(self, conn_data, connname ):
        lport, rport = conn_data
        self.Log('Got Disconnection notice for pair ({l},{r}, connection [{c}])'.format(
                                l=lport,
                                r=rport,
                                c=connname))

    def OnNoticeDISCONNECTED(self, **kwargs):
        try:
            lport = kwargs['lport']
        except KeyError:
            self.Log("OnNoticeDISCONNECTED: Missing expected parameter lport")
            return False
        try:
            rport = kwargs['rport']
        except KeyError:
            self.Log("OnNoticeDISCONNECTED: Missing expected parameter rport")
            return False

        try:
            connname = self.conn[ (lport, rport) ]
            desc = self.rconn[connname]
            if desc['servingClass']:
                desc['servingClass'].OnBrokenConnection(self, (lport, rport), connname)
            else:
                self.OnBrokenConnection( (lport, rport) )
            self.conn.pop( (lport, rport), None)
            desc['ports'] = ""
            desc['loggedin'] = False
            return True
        except KeyError:
            self.Log("OnNoticeDISCONNECTED: Missing conn/rconn for ports?")
            return False

    def InitOnNoticeCallbacks(self):
        self.onNoticeF = {
                VenueBaseCmdTraits.NOTICE_VALUE_STARTED      : ClientBaseCmdProcessor.OnNoticeSTARTED,
                VenueBaseCmdTraits.NOTICE_VALUE_FINISHED     : ClientBaseCmdProcessor.OnNoticeFINISHED,
                VenueBaseCmdTraits.NOTICE_VALUE_LISTENNING   : ClientBaseCmdProcessor.OnNoticeLISTENNING,
                VenueBaseCmdTraits.NOTICE_VALUE_CONNECTED    : ClientBaseCmdProcessor.OnNoticeCONNECTED,
                VenueBaseCmdTraits.NOTICE_VALUE_DISCONNECTED : ClientBaseCmdProcessor.OnNoticeDISCONNECTED
            }

    def StartVenueProcess(self, venue):
        self.venue = venue
        s = socket.socket()
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((socket.gethostname(), 0))
        s.listen(5)
        address = s.getsockname()

        p = Process(target=lambda:venue.StartAsServer(s))
        p.start()
        d, addr = s.accept()
        self.connected = True
        self.SetCmdSocket(d)
        self.loop()

    def OnDataFromVenue(self, *args, **kwargs):
        '''
        Specific client must provide OnMsgFromVenue function
        '''
        try:
            data = kwargs[VenueBaseCmdTraits.DATA_FIELD_NAME]
            lport = kwargs['lport']
            rport = kwargs['rport']
        except KeyError as e:
            s = "Fail extract data or ports"
            for mk, mv in kwargs.items():
                s = s + ", {}:{}".format(mk,mv)
            self.Log(s)
            raise
        if ((lport, rport) in self.conn) and self.conn[(lport, rport)]:
            cname = self.conn[(lport, rport)]
            if self.rconn[cname]['servingClass']:
                self.rconn[cname]['servingClass'].OnMsgFromVenue(self, (lport, rport), data)
                return
        self.OnMsgFromVenue((lport, rport), data)

    def OnNotice(self, *args, **kwargs):
        if not VenueBaseCmdTraits.NOTICE_KEY in kwargs:
            self.Log("No required key '{}' in parameters".format(VenueBaseCmdTraits.NOTICE_KEY));
            return False;
        try:
            return self.onNoticeF[kwargs[VenueBaseCmdTraits.NOTICE_KEY]](self, **kwargs)
        except KeyError as e:
            raise Exception("Unexpected notice event [{}]... exiting".format(kwargs[VenueBaseCmdTraits.NOTICE_KEY]))


    def RemoteLog(self, msg):
        self.LogInternal(msg)

    def LogInternal(self, msg):
        print (msg)
        sys.stdout.flush()

    def StartVenueService(self):
        self.SendCmd(VenueBaseCmdTraits.DOMAIN_NAME, 'START', {VenueBaseCmdTraits.EXCH_PORTS_KEY_NAME:self.exchports});

    def StopVenueService(self):
        self.SendCmd(VenueBaseCmdTraits.DOMAIN_NAME, 'STOP');

    def StopVenueProcess(self):
        self.SendCmd(VenueBaseCmdTraits.DOMAIN_NAME, 'KILL');

    def SendDataToServerFromVenue(self, d):
        self.SendCmd(VenueBaseCmdTraits.DOMAIN_NAME, 'D', d)

    def SendCloseSocket(self, lport, rport):
        self.SendCmd(VenueBaseCmdTraits.DOMAIN_NAME, 'C', {
                'lport':lport,
                'rport':rport
            })

    def processTO(self):
        # just wanna get out
        self.Log("Base Timeout Callback")
        self.timeoutHappened = True
        return False

    def OnDataDone(self):
        if self.timeoutHappened:
            self.timeoutHappened = False
            return False
        return True

    def PumpOutData(self):
        while(self.connected and self.OnDataDone()):
            self.loop()

    def OnCmdSocketDisconnect(self):
        #here just do nothing
        self.connected = False

def TestCmdNetCommunicator():
    cmd = CmdNetCommunicator()
    # cmd._SendOnCmdSocket("Test")

if __name__ == "__main__":
    TestCmdNetCommunicator()

