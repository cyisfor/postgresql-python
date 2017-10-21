import ctypes
from ctypes.util import find_library

from ctypes import c_int, c_void_p, c_char_p

lib = ctypes.cdll.LoadLibrary(find_library("pq"))
assert(lib)

class connection(c_void_p): pass

CONNECTION_OK, CONNECTION_BAD = range(2)


def MF(f,restype,*args):
	"makefunc"
	f.restype = restype
	if args:
		f.argtypes = args 

connect = MF(lib.PQconnectdbParams, connection)
finish = lib.PQfinish
reset = lib.PQreset
ping = lib.PQping

class OID(c_long): pass

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
class result(c_void_p): pass
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

resultStatus = MF(lib.PQresultStatus,c_char_p,result)
tuplesUpdated = MF(lib.PQcmdTuples,c_char_p,result)
resStatus = MF(lib.PQresStatus,c_char_p,c_int)

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
setErrorVerbosity = lib.PQsetErrorVerbosity
status = lib.PQstatus
errorMessage = lib.PQresultErrorMessage
errorMessage.restype = ctypes.c_char_p
connectionErrorMessage = lib.PQerrorMessage
connectionErrorMessage.restype = ctypes.c_char_p
errorField = lib.PQresultErrorField
errorField.restype = ctypes.c_char_p
clear = lib.PQclear
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
								 
putCopyEnd = lib.PQputCopyEnd
from .suckenums import *
