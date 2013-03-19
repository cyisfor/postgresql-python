from . import interface,arrayparser
from itertools import count
import datetime

class Prepared(str): pass

class SQLError(IOError):
    def __init__(self,stmt,info):
        self.stmt = stmt
        self.info = info
    def __str__(self):
        return self.stmt+'\n'+' '.join((k+'='+str(v) for k,v in self.info.items()))
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
        day = None
        try:
            year = int(result[:4])
            month = int(result[5:7])
            day = int(result[8:10])
            toffset = 11
        except ValueError:
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
        if day is None:
            if second is None: return result
            return datetime.time(hour,minute,second)
        else:
            if second is None:
                return datetime.date(year,month,day)
            else:
                # make sure it's UTC...
                hour += zone
                return datetime.datetime(year,month,day,hour,minute,second,microsecond)
    except ValueError: pass
    except IndexError: pass
    return result

class Result(list):
    error = None
    def __init__(self,rawconn,raw,stmt):
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
                raise SQLError(stmt,error)
            else:
                self.error = error
        self.fields = []
        for c in range(interface.nfields(raw)):
            self.fields.append(ctypes.string_at(interface.fname(raw,c)).decode('utf-8'))
        for r in range(interface.ntuples(raw)):
            row = list()
            for c in range(interface.nfields(raw)):
                length = interface.getlength(raw,r,c)
                if interface.getisnull(raw,r,c):
                    val = None
                else:
                    rawval = interface.getvalue(raw,r,c)
                    val = self.demogrify(rawval)
                row.append(val)
            self.append(row)
        interface.clear(raw)
    def demogrify(self,result):
        if not result: return ''
        if result[0] == ord('{') and len(result)>1 and result[-1]==ord('}'):
            return arrayparser.decode(result)
        try: return int(result)
        except ValueError: pass
        result = result.decode('utf-8')
        return maybeTimestamp(result)

stmtcounter = count(0)

def anonstatement():
    return 'anonstatement{}'.format(stmtcounter.__next__())

import ctypes
def cstrize(s):
    return ctypes.c_char_p(s.encode('utf-8'))

def makederp(typ,args):
    if not args: return None
    typ *= len(args)
    val = typ(*args)
    return val

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
        if i is None:
            return 'NULL'
        if isinstance(i,int) or isinstance(i,float):
            return str(i)
        elif isinstance(i,bytes):
            return i.decode('utf-8')
        elif isinstance(i,str):
            return i
        elif hasattr(i,'asPostgreSQL'):
            return i.asPostgreSQL(self)
        elif isinstance(i,datetime.datetime) or isinstance(i,datetime.date) or isinstance(i,datetime.time):
            return i.isoformat()
        elif isinstance(i,list) or isinstance(i,tuple) or isinstance(i,set):
            return '{'+','.join(self.mogrify(ii) for ii in i)+'}'
        elif hasattr(i,'__next__'):
            return self.mogrify(tuple(i))
        else:
            raise RuntimeError("Don't know how to mogrify type {}".format(type(i)))
    def encode(self,i):
        return self.mogrify(i).encode('utf-8')
    def execute(self,stmt,args=()):
        if isinstance(args,dict):
            # just figured out a neat trick to let %(named)s parameters
            keys = args.keys()
            subs = dict(zip(keys,['$'+str(i) for i in range(1,1+len(keys))]))
            stmt = stmt % subs
            args = [args[key] for key in keys]
            # badda boom
        args = [self.encode(arg) for arg in args]
        values = makederp(ctypes.c_char_p,args)
        lengths = makederp(ctypes.c_int,[len(arg) for arg in args])
        fmt = makederp(ctypes.c_int,(0,)*len(args))
        fullstmt = stmt
        if not isinstance(stmt,Prepared):
            types = makederp(ctypes.c_void_p,(None,)*len(args))
            if stmt in self.executedBefore:
                name = anonstatement()
                result = interface.prepare(self.raw,
                        name.encode('utf-8'),
                        stmt.encode('utf-8'),
                        len(args),
                        types)
                stmt = Prepared(name)
            else:
                result = interface.executeOnce(self.raw,
                        stmt.encode('utf-8'),
                        len(args) if args else 0,
                        types,
                        values,
                        lengths,
                        fmt,
                        0);
                self.status = interface.resultStatus(result)
                self.result = Result(self.raw,result,fullstmt)
                self.executedBefore.add(stmt)
                return self.result

        result = interface.execute(self.raw,
                stmt.encode('utf-8'),
                len(args),
                values,
                lengths,
                fmt,
                0)
        self.status = interface.resultStatus(result)
        self.result = Result(self,result,fullstmt)
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
            return self.copyIn(stmt)
        else:
            return self.copyOut(source)
    def copyIn(self,stmt):
        while True:
            buf = ctypes.c_char_p(None)
            code = interface.getCopyData(self.raw,ctypes.byref(buf),0)
            if code == -1:
                return
            elif code == -2:
                message = interface.connectionErrorMessage(self.raw).decode('utf-8')
                print(message)
                raise SQLError(message,stmt)
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
