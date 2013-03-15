import ctypes
from ctypes.util import find_library

lib = ctypes.cdll.LoadLibrary(find_library("pq"))
assert(lib)

class connection(ctypes.c_void_p): pass

connect = lib.PQconnectdbParams
connect.restype = connection
finish = lib.PQfinish
reset = lib.PQreset
ping = lib.PQping
execute = lib.PQexecPrepared
executeOnce = lib.PQexecParams
class result(ctypes.c_void_p): pass
execute.restype = executeOnce.restype = result
prepare = lib.PQprepare
resultStatus = lib.PQresultStatus
resStatus = lib.PQresStatus
resStatus.restype = ctypes.c_char_p
def escapeThing(escaper):
    def run(conn,s,len):
        result = escaper(conn,s,len(s))
        ret = ctypes.string_at(result)
        free(result)
        return ret
    return run
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
getCopyData = lib.PQgetCopyData
putCopyData = lib.PQputCopyData
putCopyEnd = lib.PQputCopyEnd
from suckenums import *
