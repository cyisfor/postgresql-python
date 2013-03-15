import interface
from itertools import count
import datetime

class Prepared(str): pass

class SQLError(IOError):
    def __init__(self,info):
        self.info = info
    def __str__(self):

        return ' '.join((k+'='+str(v) for k,v in self.info.items()))
    def __repr__(self):
        return 'SQLError('+repr(self.info)+')'

OKstatuses = set((
    interface.PGRES_COMMAND_OK,
    interface.PGRES_TUPLES_OK,
    interface.PGRES_COPY_OUT,
    interface.PGRES_COPY_IN,
    interface.PGRES_COPY_BOTH,
    interface.PGRES_SINGLE_TUPLE))

def maybeTimestamp(result):
    try:
        if result[4] != '-' and result[7] != '-' and result[10] != ' ' and result[12] != ':' and result[15] != ':':
            return result
        second = None
        try:
            year = int(result[:4])
            month = int(result[5:7])
            day = int(result[8:10])
            toffset = 11
        except ValueError:
            year = None
            toffset = 0
        try:
            hour = int(result[toffset:toffset+2])
            minute = int(result[toffset+3:toffset+5])
            secondf = None
            zone = None
            tzstart = result[toffset+8:].find('+')
            if tzstart < 0:
                tzstart = result[toffset+8:].find('-')
                if tzstart < 0:
                    secondf = float(result[toffset+7:])
                    zone = 0
            tzstart += toffset+8
            if secondf is None:
                secondf = float(result[toffset+7:tzstart])
            second = int(secondf)
            microsecond = int((secondf - second)*1000000)
            if zone is None:
                zone = int(result[tzstart+1:tzstart+3])
                if zone:
                    if result[tzstart]=='-':
                        zone = -zone
        except ValueError:
            zone = None
        if zone is not None:
            # make sure it's UTC...
            hour += zone
            return datetime.datetime(year,month,day,hour,minute,second,microsecond)
        elif second is None:
            return datetime.date(year,month,day)
        else:
            return datetime.time(hour,minute,second)
    except ValueError: pass
    except IndexError: pass
    return result

class Result(list):
    error = None
    def __init__(self,rawconn,raw):
        # no reason to leave the result sitting around in C structures now?
        self.statusId = interface.resultStatus(raw)
        resStatus = interface.resStatus(self.statusId)
        if resStatus:
            self.status = resStatus
        else:
            self.status = None
        if self.statusId not in OKstatuses:
            error = {}
            for k,v in (('message',interface.errorMessage(raw)),
                    ('severity',interface.errorField(raw,interface.PG_DIAG_SEVERITY)),
                    ('primary',interface.errorField(raw,interface.PG_DIAG_MESSAGE_PRIMARY)),
                    ('detail',interface.errorField(raw,interface.PG_DIAG_MESSAGE_DETAIL)),
                    ('hint',interface.errorField(raw,interface.PG_DIAG_MESSAGE_HINT)),
                    ('context',interface.errorField(raw,interface.PG_DIAG_CONTEXT)),
                    ('internal', interface.errorField(raw,interface.PG_DIAG_INTERNAL_QUERY))):
                error[k] = v
            position = interface.errorField(raw,interface.PG_DIAG_STATEMENT_POSITION)
            if position:
                error['position'] = (
                        int(position),
                        int(interface.errorField(raw,interface.PG_DIAG_INTERNAL_POSITION)))
            function = interface.errorField(raw,interface.PG_DIAG_SOURCE_FUNCTION)
            if function:
                error['function'] = {
                        'location': interface.errorField(raw,interface.PG_DIAG_SOURCE_FILE),
                        'line': interface.errorField(raw,interface.PG_DIAG_SOURCE_FILE),
                        'name': function,
                        }
            interface.clear(raw)
            import sys
            sys.stdout.write(error['message'].decode('utf-8'))
            if self.statusId != interface.PGRES_NONFATAL_ERROR:
                raise SQLError(error)
            else:
                self.error = error
        self.fields = []
        for c in range(interface.nfields(raw)):
            self.fields.append(ctypes.string_at(interface.fname(raw,c)))
        print(self.fields,interface.ntuples(raw))
        for r in range(interface.ntuples(raw)):
            row = list()
            for c in range(interface.nfields(raw)):
                length = interface.getlength(raw,r,c)
                rawval = interface.getvalue(raw,r,c)
                val = self.demogrify(rawval)
                row.append(val)
            self.append(row)
        interface.clear(raw)
    def demogrify(self,result):
        try: return int(result)
        except ValueError: pass
        result = result.decode('utf-8')
        return maybeTimestamp(result)

stmtcounter = count(0)

def anonstatement():
    return 'anonstatement{}'.format(stmtcounter.next())

import ctypes
def cstrize(s):
    return ctypes.c_char_p(s.encode('utf-8'))

class Connection:
    def __init__(self,**params):
        if 'params' in params:
            params.update(params['params'])
            del params['params']
        height = len(params)
        keya = ctypes.c_char_p * (height+1)
        keya = keya()
        vala = ctypes.c_char_p * (height+1)
        vala = vala()
        for i,(key,val) in enumerate(params.items()):
            keya[i] = cstrize(key)
            vala[i] = cstrize(str(val))
        keya[height] = None
        vala[height] = None
        self.raw = interface.connect(keya,vala,1)
        interface.setErrorVerbosity(self.raw,interface.PQERRORS_VERBOSE)
        self.executedBefore = set()
    def mogrify(self,i):
        if isinstance(i,int) or isinstance(i,long) or isinstance(i,float):
            return str(i)
        elif isinstance(i,bytes):
            return interface.escapeLiteral(i)
        elif isinstance(i,str):
            return interface.escapeLiteral(i.encode('utf-8'))
        elif hasattr(i,'asPostgreSQL'):
            return i.asPostgreSQL(self)
        elif isinstance(i,datetime.datetime) or isinstance(i,datetime.date) or isinstance(i,datetime.time):
            return i.isoformat()
        elif isinstance(i,list) or isinstance(i,tuple):
            return '{'+','.join(self.mogrify(ii) for ii in i)+'}'
        elif hasattr(i,__next__):
            return self.mogrify(tuple(i))
        else:
            raise RuntimeError("Don't know how to mogrify type {}".format(type(i)))
    def encode(self,i):
        return self.mogrify(i).encode('utf-8')
    def execute(self,stmt,args=()):
        args = [self.encode(arg) for arg in args]
        if not isinstance(stmt,Prepared):
            if stmt in self.executedBefore:
                name = anonstatement()
                result = interface.prepare(self.raw,
                        stmt.encode('utf-8'),
                        len(args),name.encode()
                        (ctypes.c_int_p * len(args))((None,) * len(args)))
                stmt = Prepared(name)
            else:
                if args:
                    types = (ctypes.c_char_p * len(args))(*(None,) * len(args))
                    values = (ctypes.c_char_p * len(args))(*args)
                    lengths = (ctypes.c_int * len(args))(*[len(arg) for arg in args])
                    fmt = (ctypes.c_int * len(args))(*(0,) * len(args))

                    types = ctypes.cast(types,ctypes.c_void_p)
                    values = ctypes.cast(values,ctypes.c_void_p)
                    lengths = ctypes.cast(lengths,ctypes.c_void_p)
                    fmt = ctypes.cast(fmt,ctypes.c_void_p)
                else:
                    types = values = lengths = args = fmt = None
                result = interface.executeOnce(self.raw,
                        stmt.encode('utf-8'),
                        len(args) if args else 0,
                        types,
                        values,
                        lengths,
                        fmt,
                        0);
                self.status = interface.resultStatus(result)
                self.result = Result(self.raw,result)
                self.executedBefore.add(stmt)
                return self.result
        result = interface.execute(self.raw,
                stmt.encode('utf-8'),
                len(args),
                args,
                [len(i) for i in args],
                [1]*len(args),
                1);
        self.status = interface.resultStatus(result)
        self.result = Result(result)
        return self.result
    def copy(self,stmt,source=None):
        result = interface.executeOnce(self.raw,
                stmt.encode('utf-8'),
                0,
                None,
                None,
                None,
                None,
                0)
        self.status = interface.resultStatus(result)
        if 'TO' in stmt:
            return self.copyIn()
        else:
            return self.copyOut(source)
    def copyIn(self):
        while True:
            buf = ctypes.c_char_p(None)
            code = interface.getCopyData(self.raw,ctypes.byref(buf),0)
            if code == -1:
                return
            elif code == -2:
                message = interface.connectionErrorMessage(self.raw).decode('utf-8')
                print(message)
                raise SQLError(message)
            else:
                yield ctypes.string_at(buf).decode('utf-8')
    def copyOut(self,source):
        try:
            while True:
                buf = source.read(0x1000)
                if not buf: break
                if isinstance(buf,str):
                    buf = buf.encode('utf-8')
                interface.putCopyData(self.raw,buf,len(buf))
        except Exception as e:
            interface.putCopyEnd(self.raw,str(e).encode('utf-8'))
            raise
        interface.putCopyEnd(self.raw,None)
