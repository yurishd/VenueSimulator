#!/usr/bin/python
import re
import time # for time.sleep() . if not needed then remove

MESSAGE_TYPE      = 'msgtype'
LF                = '\n'

class MsgTicker(object):
    MsgType    = MESSAGE_TYPE
    Agressor   = 'Aggressor'
    CurPair    = 'CurPair'
    Price      = 'Price'
    TxDate     = 'TxDate'
    TxTime     = 'TxTime'

class MsgMarketSnapshot(object):
    MsgType    = MESSAGE_TYPE
    MsgLength  = 'MsgLength'
    Pairs      = 'Pairs'
    CurrPair   = 'CurrPair'
    BidPxs     = 'BidPxs'
    Px         = 'Px'
    Orders     = 'Orders'
    Amount     = 'Amount'
    MinQty     = 'MinQty'
    LotSize    = 'LotSize'
    OrderID    = 'OrderID'
    OfrPxs     = 'OfrPxs'

class MsgCancelOrder(object):
    MsgType    = MESSAGE_TYPE
    CurrPair   = 'CurrPair'
    OrderID    = 'OrderID'

class MsgModifyOrder(object):
    MsgType         = MESSAGE_TYPE
    CurrPair        = 'CurrPair'
    OrderIDActive   = 'OrderIDActive'
    Price           = 'Price'
    Amount          = 'Amount'
    OrderIDReplaced = 'OrderIDReplaced'
    MinQty          = 'MinQty'
    LotSize         = 'LotSize'

class MsgNewOrder(object):
    MsgType         = MESSAGE_TYPE
    OrdSide         = 'OrdSide'
    CurrPair        = 'CurrPair'
    OrderID         = 'OrderID'
    Price           = 'Price'
    Amount          = 'Amount'
    MinQty          = 'MinQty'
    LotSize         = 'LotSize'

class MsgInstDirRequest(object):
    MsgType  = MESSAGE_TYPE
    CurrPair = 'CurrPair'

class MsgMDUnsubsRequest(object):
    MsgType  = MESSAGE_TYPE
    CurrPair = 'CurrPair'

class MsgMDSubsRequest(object):
    MsgType  = MESSAGE_TYPE
    CurrPair = 'CurrPair'

class MsgTickerUnsubsRequest(object):
    MsgType  = MESSAGE_TYPE
    CurrPair = 'CurrPair'

class MsgTickerSubsRequest(object):
    MsgType  = MESSAGE_TYPE
    CurrPair = 'CurrPair'

class MsgMarketSnapshotRequest(object):
    MsgType  = MESSAGE_TYPE
    CurrPair = 'CurrPair'

class MsgClientHeartBeat(object):
    MsgType  = MESSAGE_TYPE

class MsgLogoutRequest(object):
    MsgType  = MESSAGE_TYPE

class MsgLoginRequest(object):
    MsgType         = MESSAGE_TYPE
    LoginName       = 'LoginName'
    LoginPass       = 'LoginPass'
    MDUnsubscribe   = 'MDUnsubscribe'
    Reserved        = 'Reserved'
    ProtoMode       = 'ProtoMode'
    PxModifySupport = 'PxModifySupport'

class MsgInstDirectory(object):
    MsgType  = MESSAGE_TYPE
    Pairs    = 'Pairs'
    CurrPair = 'CurrPair'

class MsgErrorNotification(object):
    MsgType        = MESSAGE_TYPE
    ErrExplanation = 'ErrExplanation'

class MsgServerHeartBeat(object):
    MsgType        = MESSAGE_TYPE

class MsgSequencedData(object):
    MsgType  = MESSAGE_TYPE
    Time     = 'Time'
    Payload  = 'Payload'

class MsgLoginRejected(object):
    MsgType  = MESSAGE_TYPE
    Reason   = 'Reason'

class MsgLoginAccepted(object):
    MsgType           = MESSAGE_TYPE
    SeqNumberReserved = 'SeqNumberReserved'

def makeIntStr(theInt, l):
    rv = str(theInt)
    while len(rv) < l:
        rv = ' ' + rv
    return rv

def makeDoubleStr(theDouble, l):
    rv = theDouble
    while len(rv) < l:
        rv = rv + ' '
    return rv

def makeStrStr(theStr, l):
    rv = theStr
    while len(rv) < l:
        rv = rv + ' '
    return rv

def login_accepted(msg, includeOptionalFields, logger):
    return ''.join([
            msg[MsgLoginAccepted.MsgType],
            makeIntStr(msg[MsgLoginAccepted.SeqNumberReserved], 10),
            LF
        ])

def login_rejected(msg, includeOptionalFields, logger):
    return ''.join([
            msg[MsgLoginRejected.MsgType],
            makeStrStr(msg[MsgLoginRejected.Reason], 20),
            LF
        ])

def sequence_data(msg, includeOptionalFields, logger):
    if len(msg[MsgSequencedData.Payload]) :
        return ''.join([
                msg[MsgSequencedData.MsgType],
                msg[MsgSequencedData.Time],
                msg[MsgSequencedData.Payload],
                LF
            ])
    else: # End Of Session msg
        return ''.join([
                msg[MsgSequencedData.MsgType],
                LF
            ])

def server_heartbeat(msg, includeOptionalFields, logger):
    return ''.join([
            msg[MsgServerHeartBeat.MsgType],
            LF
        ])

def error_notification(msg, includeOptionalFields, logger):
    return ''.join([
            msg[MsgErrorNotification.MsgType],
            makeStrStr(msg[MsgErrorNotification.ErrExplanation], 100),
            LF
        ])

def instrument_directory(msg, includeOptionalFields, logger):
    return ''.join([
            msg[MsgInstDirectory.MsgType],
            makeIntStr(len(msg[MsgInstDirectory.Pairs]), 4),
            ''.join([ccy[MsgInstDirectory.CurrPair] for ccy in msg[MsgInstDirectory.Pairs]]),
            LF
        ])

# Client to Server. So from TCP connection (from Feed Handler -> to Server)
# Return value should be tuple, first item is parsed length, second - array of tuples
# where each tuple is key/value
def login_request(data, includeOptionalFields, logger):
    if len(data) < 92:
        return (0, None)
    return (92, [
            (MsgLoginRequest.MsgType, data[0]),
            (MsgLoginRequest.LoginName, data[1:41]), 
            (MsgLoginRequest.LoginPass, data[41:81]),
            (MsgLoginRequest.MDUnsubscribe, data[81]),
            (MsgLoginRequest.ProtoMode, data[82]),
            (MsgLoginRequest.Reserved, data[83:90]),
            (MsgLoginRequest.PxModifySupport, data[90])
           ])

def logout_request(data, includeOptionalFields, logger):
    if len(data) < 2:
        return (0, None)
    return (2, [ (MsgLogoutRequest.MsgType, data[0]), ])

def client_heartbeat(data, includeOptionalFields, logger):
    if len(data) < 2:
        return (0, None)
    return (2, [ (MsgClientHeartBeat.MsgType, data[0]), ])

def market_snapshot_request(data, includeOptionalFields, logger):
    if len(data) < 9:
        return (0, None)
    return (9, [ (MsgMarketSnapshotRequest.MsgType, data[0]), (MsgMarketSnapshotRequest.CurrPair, data[1:8]) ])

def ticker_subscribe_request(data, includeOptionalFields, logger):
    if len(data) < 9:
        return (0, None)
    return (9, [ (MsgTickerSubsRequest.MsgType, data[0]), (MsgTickerSubsRequest.CurrPair, data[1:8]) ])

def ticker_unsubscribe_request(data, includeOptionalFields, logger):
    if len(data) < 9:
        return (0, None)
    return (9, [ (MsgTickerUnsubsRequest.MsgType, data[0]), (MsgTickerUnsubsRequest.CurrPair, data[1:8]) ])

def md_subscribe_request(data, includeOptionalFields, logger):
    if len(data) < 9:
        return (0, None)
    return (9, [ (MsgMDSubsRequest.MsgType, data[0]), (MsgMDSubsRequest.CurrPair, data[1:8]) ])

def md_unsubscribe_request(data, includeOptionalFields, logger):
    if len(data) < 9:
        return (0, None)
    return (9, [ (MsgMDUnsubsRequest.MsgType, data[0]), (MsgMDUnsubsRequest.CurrPair, data[1:8]) ])

def instrument_directory_request(data, includeOptionalFields, logger):
    if len(data) < 2:
        return (0, None)
    return (2, [ (MsgInstDirRequest.MsgType, data[0]), ])

# Payloads towards FeedHandlers
#Return value should be string that goes on TCP connection

def new_order(payload, includeOptionalFields, logger):
    opt = [
            makeDoubleStr(payload[MsgNewOrder.MinQty], 16),
            makeDoubleStr(payload[MsgNewOrder.LotSize], 16)
          ] if includeOptionalFields else []
    return  ''.join([payload[MsgNewOrder.MsgType],
                 payload[MsgNewOrder.OrdSide],
             payload[MsgNewOrder.CurrPair],
             makeStrStr(payload[MsgNewOrder.OrderID], 15),
             makeDoubleStr(payload[MsgNewOrder.Price], 10),
             makeDoubleStr(payload[MsgNewOrder.Amount], 16),
            ] + opt
            )

def modify_order(payload, includeOptionalFields, logger):
    opt = [
            makeDoubleStr(payload[MsgNewOrder.MinQty], 16),
            makeDoubleStr(payload[MsgNewOrder.LotSize], 16)
          ] if includeOptionalFields else []
    if MsgModifyOrder.Price in payload:
        return ''.join([ payload[MsgModifyOrder.MsgType],
                        payload[MsgModifyOrder.CurrPair],
                        makeStrStr(payload[MsgModifyOrder.OrderIDActive], 15),
                        makeDoubleStr(payload[MsgModifyOrder.Price], 10),
                        makeDoubleStr(payload[MsgModifyOrder.Amount], 16),
                        makeDoubleStr(payload[MsgModifyOrder.OrderIDReplaced], 15)
                    ] + opt
               )
    else:
        return ''.join([ payload[MsgModifyOrder.MsgType],
                         payload[MsgModifyOrder.CurrPair],
                         makeStrStr(payload[MsgModifyOrder.OrderIDActive], 15),
                         makeDoubleStr(payload[MsgModifyOrder.Amount], 16)
                    ] + opt
                )
    
def cancel_order(payload, includeOptionalFields, logger):
    return ''.join([
            payload[MsgCancelOrder.MsgType],
            payload[MsgCancelOrder.CurrPair],
            makeStrStr(payload[MsgCancelOrder.OrderID], 15)
        ])

def market_snapshot(payload, includeOptionalFields, logger):
    # Length and type of message will be prepended later
    # All Currency Pairs
    msg = makeIntStr(len(payload[MsgMarketSnapshot.Pairs]), 4)
    for ccy in payload[MsgMarketSnapshot.Pairs]:
        msg = msg + ccy[MsgMarketSnapshot.CurrPair]
        # Bid data
        msg = msg + makeIntStr(len(ccy[MsgMarketSnapshot.BidPxs]), 4)
        for Px in ccy[MsgMarketSnapshot.BidPxs]:
            msg = msg + makeDoubleStr(Px[MsgMarketSnapshot.Px], 10)
            msg = msg + makeIntStr(len(Px[MsgMarketSnapshot.Orders]), 4)
            for Ord in Px[MsgMarketSnapshot.Orders]:
                msg = msg + makeDoubleStr(Ord[MsgMarketSnapshot.Amount], 16)
                if includeOptionalFields:
                    msg = msg + makeDoubleStr(Ord[MsgMarketSnapshot.MinQty], 16)
                    msg = msg + makeDoubleStr(Ord[MsgMarketSnapshot.LotSize], 16)
                msg = msg + makeStrStr(Ord[MsgMarketSnapshot.OrderID], 15)
        # Ofr data
        msg = msg + makeIntStr(len(ccy[MsgMarketSnapshot.OfrPxs]), 4)
        for Px in ccy[MsgMarketSnapshot.OfrPxs]:
            msg = msg + makeDoubleStr(Px[MsgMarketSnapshot.Px], 10)
            msg = msg + makeIntStr(len(Px[MsgMarketSnapshot.Orders]), 4)
            for Ord in Px[MsgMarketSnapshot.Orders]:
                msg = msg + makeDoubleStr(Ord[MsgMarketSnapshot.Amount], 16)
                if includeOptionalFields:
                    msg = msg + makeDoubleStr(Ord[MsgMarketSnapshot.MinQty], 16)
                    msg = msg + makeDoubleStr(Ord[MsgMarketSnapshot.LotSize], 16)
                msg = msg + makeStrStr(Ord[MsgMarketSnapshot.OrderID], 15)
    return payload[MESSAGE_TYPE] + makeIntStr(len(msg), 6) + msg

def ticker(payload, includeOptionalFields, logger):
    price = payload[MsgTicker.Price]
    while len(price) < 10:
        price = price + ' '
    return ''.join([
         payload[MESSAGE_TYPE],
         payload[MsgTicker.Agressor],
         payload[MsgTicker.CurPair],
         price,
         payload[MsgTicker.TxDate],
         payload[MsgTicker.TxTime]
        ])


msgToPack = {
    'A':{'name':'LOGIN_ACCEPTED',       'f':login_accepted},
    'J':{'name':'LOGIN_REJECTED',       'f':login_rejected},
    'S':{'name':'SEQUENCE_DATA',        'f':sequence_data},
    'H':{'name':'SERVER_HEARTBEAT',     'f':server_heartbeat},
    'E':{'name':'ERROR_NOTIFICATION',   'f':error_notification},
    'R':{'name':'INSTRUMENT_DIRECTORY', 'f':instrument_directory},
}
msgsToUnpack = {
    'L':{'name':'LOGIN_REQUEST',                'f':login_request},
    'O':{'name':'LOGOUT_REQUEST',               'f':logout_request},
    'R':{'name':'CLIENT_HEARTBEAT',             'f':client_heartbeat},
    'M':{'name':'MARKET_SNAPSHOT_REQUEST',      'f':market_snapshot_request},
    'T':{'name':'TICKER_SUBSCRIBE_REQUEST',     'f':ticker_subscribe_request},
    'U':{'name':'TICKER_UNSUBSCRIBE_REQUEST',   'f':ticker_unsubscribe_request},
    'A':{'name':'MD_SUBSCRIBE_REQUEST',         'f':md_subscribe_request},
    'B':{'name':'MD_UNSUBSCRIBE_REQUEST',       'f':md_unsubscribe_request},
    'I':{'name':'INSTRUMENT_DIRECTORY_REQUEST', 'f':instrument_directory_request}
}
payloadMsgs = {
    'N':{'name':'NEW_ORDER',                    'f':new_order},
    'M':{'name':'MODIFY_ORDER',                 'f':modify_order},
    'X':{'name':'CANCEL_ORDER',                 'f':cancel_order},
    'S':{'name':'MARKET_SNAPSHOT',              'f':market_snapshot},
    'T':{'name':'TICKER',                       'f':ticker}
}

class ItchProtocol(object):
    def __init__(self, logger, **kwargs):
        super(ItchProtocol, self).__init__()
        self.logger = logger
        if 'includeOptionalField' in kwargs and kwargs['includeOptionalField']:
            self.includeOptionalFields = True
        else:
            self.includeOptionalFields = False
        logger.Log('ItchProtocol created with includeOptionalField as [{o}]'.format(o=self.includeOptionalFields))

    @staticmethod
    def Name():
        return 'Itch'

    def parse(self, binmsg, debug=False):
        try:
            msgtype = binmsg[0]
        except IndexError as e:
            # No data
            return (0, None)

        desc = msgsToUnpack[msgtype]
        (parsed, ar_rv) = desc['f'](binmsg, desc, self.logger)
        if 0 == parsed:
            return (0, None)
        rv = dict(ar_rv)
        # We shouldn't pack Sequenced Data Packet
        # though just in case leave it commented out
        # # Just check whether it is Sequenced Data Packet
        # if msgtype == 'S':
        #     payload = rv['payload']
        #     if len(payload): # Not the end of session
        #         msgtype = payload[0]
        #         desc = payloadMsg[msgtype]
        #         rv['payload'] = desc['f'](payload, desc, self.logger)
        return (parsed, rv)

    def pack( self, data ):
        msgtype = data[MESSAGE_TYPE]
        if msgtype == 'S': # Sequenced Data Packet
            payload = data[MsgSequencedData.Payload]
            if len(payload) != 0:
                msgtype2 = payload[MESSAGE_TYPE]
                desc = payloadMsgs[msgtype2]
                self.logger.Log('Packing payload {n}'.format(n=desc['name']))
                data[MsgSequencedData.Payload] = desc['f'](payload, self.includeOptionalFields, self.logger)
            else:
                self.logger.Log('SENDING END OF SESSION');
                data[MsgSequencedData.Payload] = ''
        desc = msgToPack[msgtype]
        return desc['f'](data, self.includeOptionalFields, self.logger)

class LocalLogger(object):
    def Log(self, l):
        print l

def test():
    p = ItchProtocol(LocalLogger())

if __name__ == "__main__":
    test()

