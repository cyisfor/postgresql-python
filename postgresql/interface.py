import ctypes
from ctypes.util import find_library

from ctypes import c_int, c_long, c_void_p, c_char_p
from ctypes import POINTER

lib = ctypes.cdll.LoadLibrary(find_library("pq"))
assert(lib)

class connection(c_void_p): pass

from .suckenums import *

def MF(f,restype,*args):
	"makefunc"
	f.restype = restype
	if args:
		f.argtypes = args
	return f

connect = MF(lib.PQconnectStart, connection,
						 c_char_p,
						 c_int)
connectPoll = MF(lib.PQconnectPoll,PollingStatus,connection)
finish = lib.PQfinish
reset = lib.PQreset
ping = lib.PQping

class OID(c_long): pass

socket = MF(lib.PQsocket, c_int, connection)

class send:
	class noprep:
		opaque_query = MF(lib.PQsendQuery,c_int,connection,c_char_p)
		query = MF(lib.PQsendQueryParams,c_int,
							 connection,
							 c_char_p,
							 c_int,
							 POINTER(OID),
							 POINTER(c_char_p),
							 POINTER(c_int),
							 POINTER(c_int),
							 c_int)
	prepare = MF(lib.PQsendPrepare,c_int,
							 connection,
							 c_char_p,
							 c_char_p,
							 c_int,
							 POINTER(OID))
	query = MF(lib.PQsendQueryPrepared,c_int,
						 connection,
						 c_char_p,
						 c_int,
						 POINTER(c_char_p),
						 POINTER(c_int),
						 POINTER(c_int),
						 c_int)
	describe = MF(lib.PQsendDescribePrepared,c_int,c_char_p)

#execute = lib.PQexecPrepared
#executeOnce = lib.PQexecParams
#prepare = lib.PQprepare
class result(c_void_p):
	def __repr__(self):
		return repr(self.value)
	def __getattr__(self,n):
		print("ummmm",n)
		return super().__getattr__(n)
#execute.restype = executeOnce.restype = prepare.restype = result

next = MF(lib.PQgetResult,result,connection)

consume = MF(lib.PQconsumeInput,c_int,connection)
isBusy = MF(lib.PQisBusy,c_int,connection)
# call immediately after sending the query
# nah, the database has to cache these anyway, should use LIMIT to limit results
# singlerowmode = lib.PQsetSingleRowMode

class Notify(ctypes.Structure):
	_fields_ = [("name",c_char_p),
							("pid",c_int),
							("extra",c_char_p)]

notifies = MF(lib.PQnotifies,POINTER(Notify),connection)

resultStatus = MF(lib.PQresultStatus,ExecStatus,result)
tuplesUpdated = MF(lib.PQcmdTuples,c_char_p,result)
resStatus = MF(lib.PQresStatus,c_char_p,ExecStatus)

def escapeThing(escaper):
    escaper.restype = ctypes.c_void_p
    def run(conn,s):
        result = escaper(conn,s,len(s))
        ret = ctypes.string_at(result)
        lib.PQfreemem(result)
        return ret.decode('utf-8')
    return run
ftype = lib.PQftype
escapeLiteral = escapeThing(lib.PQescapeLiteral)
escapeIdentifier = escapeThing(lib.PQescapeIdentifier)
setErrorVerbosity = MF(lib.PQsetErrorVerbosity,Verbosity,
											 connection,
											 Verbosity)
status = lib.PQstatus
errorMessage = lib.PQresultErrorMessage
errorMessage.restype = ctypes.c_char_p
connectionErrorMessage = lib.PQerrorMessage
connectionErrorMessage.restype = ctypes.c_char_p
errorField = lib.PQresultErrorField
errorField.restype = ctypes.c_char_p
freeResult = lib.PQclear
nfields = lib.PQnfields
fname = lib.PQfname
fname.restype = ctypes.c_char_p
ntuples = lib.PQntuples
getlength = lib.PQgetlength
getvalue = lib.PQgetvalue
getvalue.restype = ctypes.c_char_p
getisnull = lib.PQgetisnull
getCopyData = MF(lib.PQgetCopyData,c_int,
								 connection,
								 POINTER(c_char_p),
								 c_int)
putCopyData = MF(lib.PQputCopyData,c_int,
								 connection,
								 c_char_p,
								 c_int)
								 
putCopyEnd = lib.PQputCopyEnd

port = MF(lib.PQport,c_char_p,connection)
name = MF(lib.PQdb,c_char_p,connection)

class _canceller(c_void_p): pass

canceller = MF(lib.PQgetCancel,
							 _canceller,
							 connection)

freeCancel = MF(lib.PQfreeCancel,
								None,
								_canceller);

cancel = MF(lib.PQcancel,
						c_int,
						_canceller, c_char_p, c_int)
