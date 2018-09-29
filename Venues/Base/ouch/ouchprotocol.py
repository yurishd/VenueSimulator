#!/usr/bin/python
import re
from struct import *

PACK_FORMAT='packstr'

def UnknownValue(v):
    return 'UNKNOWN ({})'.format(v)

TRUE_STR='True'
FALSE_STR='False'

HEADER_SIZE=10
HEADER_PACK = '>ciIc'

individualqt = {PACK_FORMAT:"iiqi", 'len':17}

ETX = '\x03'

def getside(side):
    if side == '1':
        return 'BID'
    if side == '2':
        return 'OFF'
    return side

def getSubsType(subsType):
    if subsType == '0':
        return 'SUBSCRIBE'
    if subsType == '1':
        return 'UNSUBSCRIBE'
    return subsType

def getInstType(instType):
    if instType == '1':
        return 'FOREIGN_EXCHANGE'
    if instType == '2':
        return 'CASH_METALS'
    return instType

def getOrdType(ordType):
    if ordType == 'D':
        return 'PREVIOUSLY QUOTED'
    if ordtype == 'F':
        return 'LIMIT'
    return ordType

def getExRptStatus(status):
    if status == 'C':
        return 'FILLED'
    return status

def getGeneralError(errCode):
    if errCode == 0:
        return 'Accepted'
    if errCode == 1:
        return 'Invalid Instrument'
    if errCode == 2:
        return 'Invalid Side'
    if errCode == 3:
        return 'Invalid Price'
    if errCode == 4:
        return 'Invalid Expiry'
    if errCode == 5:
        return 'Invalid Amount'
    if errCode == 6:
        return 'Invalid Show Amount'
    if errCode == 7:
        return 'Invalid Permission'
    if errCode == 9:
        return 'Invalid Order Type'
    if errCode == 10:
        return 'Invalid clOrdID'
    if errCode == 11:
        return 'Invalid Credit'
    if errCode == 12:
        return 'Max Permitted Ops Exceeded'
    if errCode == 13:
        return 'Invalid New Client Order, already filled'
    if errCode == 14:
        return 'Order Not Active'
    if errCode == 15:
        return 'Invalid Specified Amount'
    if errCode == 16:
        return 'Max # of Active Orders Exceeded'
    if errCode == 17:
        return 'Rate Precision Error'
    if errCode == 18:
        return 'Invalid Settlement Date'
    if errCode == 99:
        return 'Invalid Error'
    return UnknownValue(errCode)

def pad_right(s, n):
    cn = n - len(s)
    if cn > 0:
        return s + (' ' * cn)
    return s

# message parsers
def header_arr(seqN, ts, msgtype, name):
    return [('msgtype', msgtype), ('msgname',name), ('seqN',str(seqN)), ('timestamp',ts)]

def parse_logon(msg, desc):
    (userid, pswrd, etx) = unpack(desc[PACK_FORMAT], msg[:desc['len']])
    return (desc['len'], [('userid',userid.rstrip()), ('pswrd',pswrd.rstrip())])

def pack_logon(data, desc, logger):
    m = pack(desc[PACK_FORMAT], pad_right(data['userid'], 20), pad_right(data['pswrd'], 20), ETX)
    logger.Log('pack_logon msg {} length'.format(len(m)))
    return m

def parse_logout(msg, desc):
    ( userid,reason, etx) = unpack(desc[PACK_FORMAT], msg[:desc['len']])
    return (desc['len'], [('userid',userid), ('reason',pswrd)])

def pack_logout(data, desc, logger):
    m = pack(desc[PACK_FORMAT], pad_right(data['userid'], 20), pad_right(data['reason'], 3), ETX)
    logger.Log('pack_logout msg {} length'.format(len(m)))
    return m

def parse_heartbeat(msg, desc):
    # ( etx, ) = unpack(desc[PACK_FORMAT], msg[:desc['len']])
    return (desc['len'], [] )
    
def pack_heartbeat(data, desc, logger):
    # ( etx, ) = unpack(desc[PACK_FORMAT], msg[:desc['len']])
    m = pack(desc[PACK_FORMAT], ETX)
    logger.Log('pack_heartbeat msg {} length'.format(len(m)))
    return m
    
def parse_testrequest(msg, desc):
    ( reqid, etx) = unpack(desc[PACK_FORMAT], msg[:desc['len']])
    return  (desc['len'], [('requestid',str(reqid))])

def pack_testrequest(data, desc, logger):
    m = pack(desc[PACK_FORMAT], int(data['requestid']), ETX)
    logger.Log('pack_testrequest msg {} length'.format(len(m)))
    return m

def parse_testresponse(msg, desc):
    ( reqid, etx) = unpack(desc[PACK_FORMAT], msg[:desc['len']])
    return (desc['len'], [('requestid',str(reqid))])

def pack_testresponse(data, desc, logger):
    m = pack(desc[PACK_FORMAT], int(data['requestid']), ETX)
    logger.Log('pack_testresponse msg {} length'.format(len(m)))
    return m

def rejectType (rjt):
    if rjt == '0':
        return 'Invalid Field Format'
    if rjt == '1':
        return 'Invalid Message Format'
    if rjt == '2':
        return 'Internal Error'
    if rjt == '\0':
        return 'Invalid Instrument Info ID'
    return UnknownValue(rjt)

def parse_reject(msg, desc):
    ( rejseqn, rejType, reason, etx) = unpack(desc[PACK_FORMAT], msg[:desc['len']])
    return (desc['len'], [('rejseqn',str(rejseqn)),('rejectType',rejectType(rejType)),('reason',reason)])

def pack_reject(data, desc, logger):
    m = pack(desc[PACK_FORMAT], int(data['rejseqn']), data['rejectType'], data['reason'], ETX)
    logger.Log('pack_reject msg {} length'.format(len(m)))
    return m

def parse_instinforequest(msg, desc):
    # (etx,) = unpack(desc[PACK_FORMAT], msg[:desc['len']])
    return (desc['len'], [])

def pack_instinforequest(data, desc, logger):
    # Just empty
    m = pack(desc[PACK_FORMAT], ETX)
    logger.Log('pack_instinforequest msg {} length'.format(len(m)))
    return m

def parse_instinfo(msg, desc):
    ( instinfoID, side, subsType, instIndex, instType, instID, amount, settleTS, streamrefID, etx) = \
                     unpack(desc[PACK_FORMAT], msg[:desc['len']])
    return (desc['len'], [
            ('instinfoID',str(instinfoID)),
            ('side',getside(side)),
            ('subsType',getSubsType(subsType)),
            ('instIndex',str(instIndex)),
            ('instType',getInstType(instType)),
            ('instID',instID),
            ('amount',str(amount)),
            ('settleTS',str(settleTS)),
            ('streamrefID',streamrefID)
        ])

def pack_instinfo(data, desc, logger):
    m = pack(desc[PACK_FORMAT],
             int(data['instinfoID']),
             data['side'],
             data['subsType'],
             int(data['instIndex']),
             data['instType'],
             pad_right(data['instID'], 20),
             int(data['amount']),
             int(data['settleTS']),
             pad_right(data['streamrefID'], 20),
             ETX
            )
    logger.Log('pack_instinfo msg {} length'.format(len(m)))
    return m

def getinstinfoAckType(instinfoAckType):
    if instinfoAckType == '1':
        return 'ACCEPT'
    if instinfoAckType == '2':
        return 'REJECT'
    return UnknownValue(instinfoAckType)

def parse_instinfoack(msg, desc):
    ( instinfoID, instinfoAckType, errCode, reason, etx) = unpack(desc[PACK_FORMAT], msg[:desc['len']])
    return (desc['len'], [
            ('instinfoID',instinfoID),
            ('instinfoAckType',getinstinfoAckType(instinfoAckType)),
            ('errCode',getGeneralError(errCode)),
            ('reason',reason)
        ])

def pack_instinfoack(data, desc, logger):
    m = pack(desc[PACK_FORMAT],
            int(data['instinfoID']),
            data['instinfoAckType'],
            int(data['errCode']),
            pad_right(data['reason']),
            ETX
            )
    logger.Log('pack_instinfoack msg {} length'.format(len(m)))
    return m

def parse_quote(msg, desc):
    ( quoteID, instinfoID, amount, rate, etx) = unpack(desc[PACK_FORMAT], msg[:desc['len']])
    return (desc['len'], [
            ('quoteID',str(quoteID)),
            ('instinfoID',str(instinfoID)),
            ('amount',str(amount)),
            ('rate',str(rate))
        ])

def pack_quote(data, desc, logger):
    m = pack(desc[PACK_FORMAT],
            int(data['quoteID']),
            int(data['instinfoID']),
            int(data['amount']),
            int(data['rate']),
            ETX
            )
    logger.Log('pack_quote msg {} length'.format(len(m)))
    return m

def parse_quotecancel(msg, desc):
    (instinfoID, etx) = unpack(desc[PACK_FORMAT], msg[:desc['len']])
    return (desc['len'], [('instinfoID',str(instinfoID))])

def pack_quotecancel(data, desc, logger):
    m = pack(desc[PACK_FORMAT],
            int(data['instinfoID']),
            ETX
            )
    logger.Log('pack_quotecancel msg {} length'.format(len(m)))
    return m

def parse_massquote(msg, desc):
    lpos = desc['len'] # 2 is nQuotes
    (nQuotes,) = unpack(desc[PACK_FORMAT], msg[:lpos])
    rv = [('QuotsCount', str(nQuotes))]
    i = 0;
    while i < nQuotes:
        rpos = lpos
        lpos = lpos + individualqt['len']
        (quoteID,instinfoID, amount,rate) = unpack(individualqt[PACK_FORMAT], msg[rpos, lpos])
        rv.append( ('quoteID',str(quoteID)))
        rv.append( ('instinfoID',str(instinfoID)))
        rv.append( ('amount',str(amount)))
        rv.append( ('rate',str(rate)))
    return (lpos, rv)

def pack_massquote(data, desc, logger):
    m = pack(desc[PACK_FORMAT], len(data['QuotsCount']))
    for q in data['quotes']:
        d = dict(q)
        m = m + pack(individualqt[PACK_FORMAT],
                    int(d['quoteID']),
                    int(d['instinfoID']),
                    int(d['amount']),
                    int(d['rate'])
                    )
    m = m + pack('c', ETX)
    logger.Log('pack_massquote msg {} length'.format(len(m)))
    return m

def IsBase(base):
    if base == '1':
        return TRUE_STR
    if base == '2':
        return FALSE_STR
    return UnknownValue(base)

def parse_neworder(msg, desc):
    (clOrdID, instID, side, ordMinAmt, ordAmt, streamrefID, quoteID, px,
         ordType, clntID, base, execFirm, settleTS, etx) = unpack(desc[PACK_FORMAT], msg[:desc['len']])
    return (desc['len'], [
            ('clOrdID',str(clOrdID)),
            ('instID',instID),
            ('side',getside(side)),
            ('ordMinAmt',str(ordMinAmt)),
            ('ordAmt',str(ordAmt)),
            ('streamrefID',streamrefID),
            ('quoteID',str(quoteID)),
            ('px',str(px)),
            ('ordType',getOrdType(ordType)),
            ('clntID',clntID),
            ('IsBaseSpecified',IsBase(base)),
            ('execFirm',execFirm),
            ('settleTS',str(settleTS))
        ])

def adjust_side(s):
    '''
    OUCH protocol reserves values '1' and '2' as sides for messages INSTRUMENT INFO
    Although, for NEW_ORDER message it requires values 'B' and 'S'.
    Since we are not to argue about it and for simplicity test creation we just internally
    ready to convert properly
    '''
    if s == '1':
        return 'B'
    if s == '2':
        return 'S'
    # Otherwise just return what it was
    return s

def pack_neworder(data, desc, logger):
    m = pack(desc[PACK_FORMAT],
            int(data['clOrdID']),
            pad_right(data['instID'], 20),
            adjust_side(data['side']), # Surprisingly ouch protocol has different side values for instr info and NOS
            int(data['ordMinAmt']),
            int(data['ordAmt']),
            pad_right(data['streamrefID'], 20),
            int(data['quoteID']),
            int(data['px']),
            data['ordType'],
            pad_right(data['clntID'], 20),
            data['IsBaseSpecified'],
            pad_right(data['execFirm'], 20),
            int(data['settleTS']),
            ETX
            )
    logger.Log('pack_neworder msg {} length'.format(len(m)))
    return m

def parse_exrpt(msg, desc):
    (clOrdID, execID, status, fillAmount, amountLeft, fillRate, etx) = unpack(desc[PACK_FORMAT], msg[:desc['len']])
    return (desc['len'], [
            ('clOrdID',str(clOrdID)),
            ('execID',execID),
            ('status',getExRptStatus(status)),
            ('fillAmount',str(fillAmount)),
            ('amountLeft',str(amountLeft)),
            ('fillRate',str(fillRate))
        ])

def pack_exrpt(data, desc, logger):
    m = pack(desc[PACK_FORMAT],
            int(data['clOrdID']),
            pad_right(data['execID'], 50),
            data['status'], # expected to be just 'C'
            int(data['fillAmount']),
            int(data['amountLeft']),
            int(data['fillRate']),
            ETX
            )
    logger.Log('pack_exrpt msg {} length'.format(len(m)))
    return m

def rejErrorCode(code):
    if code == 0:
        return 'Price unavailable'
    if code == 1:
        return 'Invalid Offer'
    if code == 2:
        return 'Not enough credit'
    if code == 153: # 0x99
        return 'Invalid error'
    return UnknownValue(code)

def parse_exrptreject(msg, desc):
    (clOrdID, errCode, etx) = unpack(desc[PACK_FORMAT], msg[:desc['len']])
    return (desc['len'], [
        ('clOrdID', str(clOrdID)),
        ('errCode',rejErrorCode(errCode))
    ])

def pack_exrptreject(data, desc, logger):
    m = pack(desc[PACK_FORMAT],
            int(data['clOrdID']),
            int(data['errCode']),
            ETX
            )
    logger.Log('pack_exrpt_reject msg {} length'.format(len(m)))
    return m

def is_aggressor(aggr):
    if aggr == '1':
        return TRUE_STR
    if aggr == '2':
        return FALSE_STR
    return UnknownValue(aggr)


def parse_exrpt_ack(msg, desc):
    (clOrdId, execOrdId, isAggressor, lmUID, etx) = unpack(desc[PACK_FORMAT], msg[:desc['len']])
    return  (desc['len'], [
        ('clOrdId',str(clOrdId)),
        ('execOrdId',execOrdId),
        ('isAggressor', is_aggressor(isAggressor)),
        ('LMUID',lmUID)
    ])

def pack_exrpt_ack(data, desc, logger):
    m = pack(desc[PACK_FORMAT],
            int(data['clOrdId']),
            pad_right(data['execOrdId'], 20),
            data['isAggressor'],
            pad_right(data['LMUID'],20),
            ETX
            )
    logger.Log('pack_exrpt_ack msg {} length'.format(len(m)))
    return m

def parse_exrpt_nack(data, desc):
    (clOrdId, ExecutionID, errorCode, etx) = unpack(desc[PACK_FORMAT], msg[:desc['len']])
    return (desc['len'], [
        ('clOrdId',str(clOrdId)),
        ('ExecutionID',ExecutionID),
        ('errorCode',getGeneralError(errorCode))
        ])

def pack_exrpt_nack(msg, desc, logger):
    m = pack(desc[PACK_FORMAT],
            int(data['clOrdId']),
            pad_right(data['ExecutionID'],20),
            int(data['errorCode']),
            ETX
            )
    logger.Log('pack_exrpt_nack msg {} length'.format(len(m)))
    return m

def parse_exrptreject_ack(msg, desc):
    (clOrdId, ExecutionID, lmUID, etx) = unpack(desc[PACK_FORMAT], msg[:desc['len']])
    return (desc['len'], [
        ('clOrdId',str(clOrdId)),
        ('ExecutionID',ExecutionID),
        ('LMUID',lmUID)
        ])

def pack_exrptreject_ack(data, desc, logger):
    m = pack(desc[PACK_FORMAT],
            int(data['clOrdId']),
            pad_right(data['ExecutionID'],20),
            pad_right(data['LMUID'],20),
            ETX
            )
    logger.Log('pack_exrptreject_ack msg {} length'.format(len(m)))
    return m

def parse_exrptreject_nack(msg, desc):
    (clOrdId, ExecutionID, errCode, etx) = unpack(desc[PACK_FORMAT], msg[:desc['len']])
    return (desc['len'], [
        ('clOrdId',str(clOrdId)),
        ('ExecutionID',ExecutionID),
        ('errCode',getGeneralError(errCode))
        ])

def pack_exrptreject_nack(data, desc, logger):
    m = pack(desc[PACK_FORMAT],
            int(data['clOrdId']),
            pad_right(data['ExecutionID'], 20),
            int(data['errCode']),
            ETX
            )
    logger.Log('pack_exrptreject_nack msg {} length'.format(len(m)))
    return m

def parse_trade_to(msg, desc):
    (clOrdId, etx) = unpack(desc[PACK_FORMAT], msg[:desc['len']])
    return (desc['len'], [('clOrdId',str(clOrdId))])

def pack_trade_to(data, desc, logger):
    m = pack(desc[PACK_FORMAT],
            int(data['clOrdId']),
            ETX
            )
    logger.Log('pack_trade_to msg {} length'.format(len(m)))
    return m

def parse_dontknowtrade(msg, desc):
    (clOrdId,  etx) = unpack(desc[PACK_FORMAT], msg[:desc['len']])
    return (desc['len'], [('clOrdId',str(clOrdId))])

def pack_dontknowtrade(data, desc, logger):
    m = pack(desc[PACK_FORMAT],
            int(data['clOrdId']),
            ETX
            )
    logger.Log('pack_trade_to msg {} length'.format(len(m)))
    return m

msgs = {
    'A':{'name':'LOGON',                        PACK_FORMAT:"!20s20sc",               'len':41,   'f':parse_logon,            'p':pack_logon},
    'B':{'name':'LOGOUT',                       PACK_FORMAT:"!20s3sc",                'len':24,   'f':parse_logout,           'p':pack_logout},
    'C':{'name':'HEARTBEAT',                    PACK_FORMAT:"!c",                     'len':1,    'f':parse_heartbeat,        'p':pack_heartbeat},
    '1':{'name':'TEST_REQUEST',                 PACK_FORMAT:"!ic",                    'len':5,    'f':parse_testrequest,      'p':pack_testrequest},
    '0':{'name':'TEST_RESPONSE',                PACK_FORMAT:"!ic",                    'len':5,    'f':parse_testresponse,     'p':pack_testresponse},
    'K':{'name':'REJECT',                       PACK_FORMAT:"!ic20sc",                'len':26,   'f':parse_reject,           'p':pack_reject}, 
    'D':{'name':'INSTRUMENT_INFO_REQUEST',      PACK_FORMAT:"!c",                     'len':1,    'f':parse_instinforequest,  'p':pack_instinforequest},
    'E':{'name':'INSTRUMENT_INFO',              PACK_FORMAT:"!icchc20sqq20sc",        'len':66,   'f':parse_instinfo,         'p':pack_instinfo},
    'F':{'name':'INSTRUMENT_INFO_ACK',          PACK_FORMAT:"!ich20sc",               'len':28,   'f':parse_instinfoack,      'p':pack_instinfoack},
    'h':{'name':'QUOTE',                        PACK_FORMAT:"!iiqic",                 'len':21,   'f':parse_quote,            'p':pack_quote},
    'Z':{'name':'QUOTE_CANCEL',                 PACK_FORMAT:"!ic",                    'len':5,    'f':parse_quotecancel,      'p':pack_quotecancel},
    'i':{'name':'MASS_QUOTE',                   PACK_FORMAT:"!h",                     'len':2,    'f':parse_massquote,        'p':pack_massquote},
    'l':{'name':'NEW_ORDER',                    PACK_FORMAT:"!i20scqq20siic20sc20sqc",'len':120,  'f':parse_neworder,         'p':pack_neworder},
    'm':{'name':'EXECUTION_REPORT',             PACK_FORMAT:"!i50scqqic",             'len':76,   'f':parse_exrpt,            'p':pack_exrpt},
    'W':{'name':'EXECUTION_REPORT_REJECT',      PACK_FORMAT:"!ihc",                   'len':7,    'f':parse_exrptreject,      'p':pack_exrptreject},
    't':{'name':'EXECUTION_REPORT_ACK',         PACK_FORMAT:"!i20sc20sc",             'len':46,   'f':parse_exrpt_ack,        'p':pack_exrpt_ack},
    'u':{'name':'EXECUTION_REPORT_NACK',        PACK_FORMAT:"!i20shc",                'len':27,   'f':parse_exrpt_nack,       'p':pack_exrpt_nack},
    'v':{'name':'EXECUTION_REPORT_REJECT_ACK',  PACK_FORMAT:"!i20s20sc",              'len':45,   'f':parse_exrptreject_ack,  'p':pack_exrptreject_ack},
    'w':{'name':'EXECUTION_REPORT_REJECT_NACK', PACK_FORMAT:"!i20shc",                'len':27,   'f':parse_exrptreject_nack, 'p':pack_exrptreject_nack},
    'U':{'name':'TRADE_TIMED_OUT',              PACK_FORMAT:"!ic",                    'len':5,    'f':parse_trade_to,         'p':pack_trade_to},
    'T':{'name':'DONT_KNOW_TRADE',              PACK_FORMAT:"!ic",                    'len':5,    'f':parse_dontknowtrade,    'p':pack_dontknowtrade}
}

SOH = '\x01' 

class OuchProtocol(object):
    def __init__(self, logger):
        super(OuchProtocol, self).__init__()
        self.logger = logger

    @staticmethod
    def Name():
        return 'Ouch'

    def parse(self, binmsg, debug=False):
        if len(binmsg) < HEADER_SIZE:
            return 0, None;
        hpack = HEADER_PACK # + 'x'*(len(binmsg) - HEADER_SIZE)
        if debug:
            self.logger.Log("parse_ouch msg {} length [{}]".format(len(binmsg), binmsg))
        try:
            (soh, seqN, ts, msgtype) = unpack(hpack, binmsg[:HEADER_SIZE])
        except:
            self.logger.Log( "Fail parse header {}".format(len(binmsg)))
            return -1, None

        if debug:
            self.logger.Log("Trying to get desc")
        try:
            desc = msgs[msgtype]
        except KeyError:
            self.logger.Log('UNEXPECTED Msg [{}][{}][{}][{}] [{}][{}][{}][{}][{}][{}][{}][{}][{}][{}] [{}]'.format(msgtype, ord(msgtype), seqN, ts,
                                                ord(binmsg[0]),ord(binmsg[1]),ord(binmsg[2]),ord(binmsg[3]),ord(binmsg[4]),
                                                ord(binmsg[5]),ord(binmsg[6]),ord(binmsg[7]),ord(binmsg[8]),ord(binmsg[9]),
                                                                                                     binmsg))
            return -1, None 
        if debug:
            self.logger.Log("Checking length")
        if desc['len'] + HEADER_SIZE > len(binmsg):
            self.logger.Log("Too short to parse msg [{}][{}]. Length is [{}], expected [{}]".format(desc['name'],binmsg, len(binmsg), desc['len'] + HEADER_SIZE))
            return 0, None

        if debug:
            self.logger.Log("Before call for {}".format(desc['name'].lower()))
        (parsed, ar_rv) = desc['f'](binmsg[HEADER_SIZE:], desc)
        return (parsed + HEADER_SIZE, dict(header_arr(seqN, ts, msgtype,desc['name']) + ar_rv))     

    def pack( self, data ):
        msgtype = data['msgtype']
        header = pack(HEADER_PACK, SOH, int(data['seqN']), int(data['timestamp']), msgtype)
        desc = msgs[msgtype]
        return header + desc['p'](data, desc, self.logger)

class LocalLogger(object):
    def Log(self, l):
        print l

def test():
    p = OuchProtocol(LocalLogger())

if __name__ == "__main__":
    test()
