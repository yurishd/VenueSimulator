#!/usr/bin/python
import re
from struct import *
import pdb

fieldpttn=re.compile("(.*?)=(.*)");

class MFFixProtocol(object):
    CUSTOM_MSG_TYPE = '1024'
    def __init__(self, logger):
        super(MFFixProtocol, self).__init__()
        self.logger = logger

    @staticmethod
    def Name():
        return 'MFFix'

    def parse_field(self, binmsg, debug):
        rv = 0;
        for ch in binmsg:
            (b, ) = unpack("b", ch)
            #if (debug):
            #    self.logger.Log(b)
            if b == 1:
                m = fieldpttn.match(binmsg[:rv])
                if not m: # very bad! something wrong
                    raise Exception("Fail to extract field from [{m}] even having SOH at index [{indx}]".format(m=binmsg, indx=rv))
                return (m.group(1), m.group(2), rv+1)
            rv = rv + 1
        return (None, None, 0)
                
    def parse_header(self, binmsg, debug):
        (protoTag,protoValue,protoParsed) = self.parse_field(binmsg, debug)
        if not protoParsed:
            if (debug):
                self.logger.Log("No PROTO detected")
            return (0, 0, None)
        (lenTag,lenValue,lenParsed) = self.parse_field(binmsg[protoParsed:], debug)
        if not lenParsed:
            if (debug):
                self.logger.Log("No LEN detected")
            return (0, 0, None)
        while len(lenValue):
            if lenValue[0] == '0':
                lenValue = lenValue[1:]
            else:
                break
        if len(lenValue) == 0:
            lenValue = 0
        else:
            lenValue = int(lenValue)
        return (protoValue, lenValue, lenParsed+protoParsed)

    def checkSumCal(self, msg):
        chsum = 0
        curindex = 0
        nbytes = len(msg)
        while nbytes > 1:
            (b,r) = unpack("B{}s".format(nbytes-1), msg[curindex:])
            chsum    = chsum + b
            curindex = curindex + 1
            nbytes   = nbytes-1
        (b, ) = unpack("B", msg[curindex:])
        chsum = (chsum + b)%256
        toTest = ""
        if chsum < 10:
            toTest = "00"
        elif chsum < 100:
            toTest = "0"
        return  toTest + str(chsum)
        
    def testChecksum(self, msg, valueToCompare, debug):
        ch = self.checkSumCal(msg)
        if ch == valueToCompare:
            return True
        self.logger.Log("testChecksum fails {ch} {v}".format(ch=ch,v=valueToCompare))
        return False

    def ErrorMsg(self, txt):
        return [('8', 'UNKNOWN'), ('9','1024'), ('35', MFFixProtocol.CUSTOM_MSG_TYPE), ('58',txt)]

    def parse(self, binmsg, debug=False):
        (proto, msglen, headerParsed) = self.parse_header(binmsg, debug)
        if not headerParsed: # not enough to have header
            if (debug):
                self.logger.Log("No header detected")
            return (0, None)
        if msglen + headerParsed + 1 > len(binmsg): # message is not complete yet. "+1" needed for checksum
            if (debug):
                self.logger.Log("No body detected")
            return (0, None)
        (checksumTag, checksumValue, checksumParsed) = self.parse_field(binmsg[msglen + headerParsed:], debug)
        if not checksumParsed: # No checksum yet in buffer
            if (debug):
                self.logger.Log("No checksum detected")
            return (0, None)

        # If we're here then we have our buffer complete msg
        # pdb.set_trace()
        if not self.testChecksum(binmsg[:msglen + headerParsed], checksumValue, debug):
            msg = binmsg[:msglen + headerParsed + checksumParsed]
            msg.replace('\x01', '|')
            return (msglen + headerParsed + checksumParsed, self.ErrorMsg('Check sum calc failed for msg [{m}]'.format(m=msg)))
        rv = []
        nbytes = headerParsed + msglen + checksumParsed
        msg = binmsg
        currindex = 0
        while nbytes > currindex:
            (tag,value,parsed) = self.parse_field(binmsg[currindex:], debug)
            if not parsed:
                return (msglen + headerParsed + checksumParsed, self.ErrorMsg('Fail to parse field at index [{indx}]'.format(indx=currindex)))
            rv.append((tag,value))
            currindex = currindex + parsed
        return (nbytes, rv)

    def pack( self, data ):
        msg = '\x01'.join(['{t}={v}'.format(t=t,v=v) for t,v in data])
        msg = msg + '\x01'
        msg = msg + '10={c}\x01'.format(c=self.checkSumCal(msg))
        return msg

class LocalLogger(object):
    def Log(self, l):
        print l

def test():
    logger = LocalLogger()
    proto = MFFixProtocol(logger)
    src = "8=FIXT.1.1|9=80|35=0|34=105993|1128=9|49=TR MATCHING|56=AAAN021013BBAB|52=20171215-19:13:00.619|10=192|"
    msg = src.replace('|', '\x01')
    (parsed, rv) = proto.parse(msg, True)
    print "Three lines below should be identical"
    print src
    print '|'.join(["{k}={v}".format(k=k,v=v) for (k,v) in rv])+'|'
    msg = proto.pack(rv[:-1])
    print msg.replace('\x01', '|')

    src2 = "8=FIXT.1.1|9=00140|35=BE|49=YSH-TEST-OF|56=YSHVENUE|34=75|50=1|42=YSHLocationID|57=FXM|52=20180102-23:42:20.394|923=userReq1514936540394852326|924=1|553=|554=|10=005|"
    msg = src2.replace('|', '\x01')
    (parsed, rv) = proto.parse(msg, True)
    print "Three lines below should be identical"
    print src2
    print '|'.join(["{k}={v}".format(k=k,v=v) for (k,v) in rv])+'|'
    msg = proto.pack(rv[:-1])
    print msg.replace('\x01', '|')

if __name__ == "__main__":
    test()
