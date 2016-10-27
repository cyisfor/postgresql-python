# uggggh
import struct

from . import main

def U(fmt, message):
	ret = struct.unpack("!"+fmt,message)
	if len(ret) == 0:
		return ret[0]
	return ret

def receive():
	typ = main.c.read(1)
	length = struct.unpack("!L",inp.read(4))
	length -= 4 # length includes itself
	return typ, inp.read(length)

def startup(**kw):
	import frontend as F
	import backend as B
	F.StartupMessage(kw)
	type,message = read_message()
	if type == B.ErrorResponse:


def raw_read(inp):
	typ = inp.read(1)
	length = struct.unpack("!L",inp.read(4))
	length -= 4 # length includes itself
	return typ, inp.read(length)

def raw_write(out,typ,contents):
	out.write(typ + struct.pack("!L",len(contents)) + contents)

class Authentication(Message):
	def __init__(self, message):
		self.subtype = message[:4]
		self.subtype = struct.unpack('!L',subtype)
		if self.subtype == 5:
			self.salt = message[4:]
		elif self.subtype == 8:
			self.gssapifuckery = message[4:]
			
class BackendKeyData(Message):
	def __init__(self, message):
		self.pid, self.key = struct.unpack("!LL",message)

def little_array(message, size=2, incsize=2):
	if incsize == 2:
		derp = "!H"
	elif incsize == 4:
		derp = "!L"
	elif incsize == 1:
		derp = "b"
	num = struct.unpack("!H",message[:size])
	message = message[size:]
	pos = 0
	result = [None]*num
	for i in range(num):
		length = struct.unpack(derp, message[pos:pos+incsize])
		result[i] = message[pos+incsize:pos+incsize+length]
		pos += incsize + length
	return result, message[pos:]

def derp(size):
	if size == 2:
		return "H"
	elif size == 4:
		return "L"
	elif size == 1:
		return "b"

def parse_ints(message,incsize=2,size=2):
	num = struct.unpack(derp(size),message[:size])
	message = message[size:]
	result = [0]*num
	incderp = derp(incsize)*num
	result = struct.unpack("!"+incderp, message)
	return result, message[size+incsize*(num+1)]

def encode_ints(ints,incsize=2,size=2):
	incderp = derp(incsize) * len(ints)
	return (struct.pack(derp(size),len(ints)) +
	        b''.join(struct.pack(incderp,*ints)))

def frontend(typ):
	typ = typ.encode('utf-8')
	def deco(f):
		def wrapper(*a,**kw):
			mess = f(*a,**kw) or ""
			return typ + struct.pack("!L",len(mess))+mess
		return wrapper
	return deco

@frontend('B')
def Bind(dest, source, formats, values, result_formats):
	return (dest.encode('utf-8') + b'\0' + source.encode('utf-8') +
	        encode_ints(formats) +
	        encode_little_array(values) +
	        encode_ints(result_formats))
	def __init__(self, name, ):
		self.name,message = message.split(0,1)
		self.source,message = message.split(0,1)
		self.formats,message = parse_ints(message)

		self.values, message = little_array(message)
		# incoming parameters have a format, and outgoing results have a format
		self.result_formats,message = parse_ints(message)

class BindComplete(Message): pass

def CancelRequest(target,secret):
	mess = struct.pack("!lll",80877102,target,secret)
	return struct.pack("!L",len(mess))+mess

@frontend('C')
def Close(is_portal, name):
	return (b'P' if is_portal else b'S') + name.encode('utf-8')

class CloseComplete(Message): pass
class CommandComplete(Message):
	def __init__(self, message):
		message = message.decode("utf-8")
		self.command,*message = message.split(" ")
		if self.command == "INSERT":
			self.oid, self.rows = message
		else:
			self.rows = message[0]
			
class CopyData(Message):
	def __init__(self, message):
		self.data = message
	@frontend('d')
	def __call__(self, data):
		return data

class CopyDone(Message):
	typ = 'c'
	@frontend(CopyDone.typ)
	def __call__(self): pass

@frontend('f')
def CopyFail(error):
	return error.encode('utf-8')+b'\0'
	
class CopyResponse(Message):
	def __init__(self, message):
		self.is_binary = message[0] == 0
		self.formats = parse_ints(message)
		
class CopyInResponse(CopyResponse): pass
class CopyOutResponse(CopyResponse): pass
class CopyBothResponse(CopyResponse): pass

class DataRow(Message):
	def __init__(self, message):
		self.values,message = little_array(message, 2, 4)

@frontend('D')
def Describe(is_portal, name):
	return (b'P' if is_portal else b'S') + name.encode("utf-8") + b'\0'
		
class EmptyQueryResponse(Message): pass

class ResponseWithFields(Message):
	def __init__(self, message):
		self.fields = little_array
		# 0 terminator means len - 1
		while pos < len(message) - 1:
			length = message[pos]
			self.fields.append(message[pos+1:pos+1+length])
			pos += 1 + length

class ErrorResponse(ResponseWithFields): pass

@frontend('E')
def Execute(name, max_rows=0):
	return name.encode('utf-8')+b'\0'+struct.pack('!L',max_rows)

@frontend('H')
def Flush(): pass

@frontend('F')
def FunctionCall(oid, formats, arguments, result_format):
	return (struct.pack("!L",oid) + encode_ints(formats)
	        + encode_little_array(arguments,4)
	        + struct.pack("!H", result_format))

class FunctionCallResponse(Message):		
	def __init__(self, message):
		almostuseless = struct.unpack("!l", message[:4])
		if almostuseless == -1:
			self.null = True
		else:
			self.result = message[4:]

class NoData(Message): pass
class NoticeResponse(ResponseWithFields): pass
class StringPair(Message):
	def __init__(self, message):
		self.pid = struct.unpack("!L",message[:4])
		self.name, message = message.split(0,1)
		self.value, message = message.split(0,1)
		return message
class NotificationResponse(StringPair): pass
class ParameterDescription(Message):
	def __init__(self, message):
		self.num = struct.unpack("!H",message[:2])
		assert self.num == (len(message)-2)/4
		self.types = [0] * self.num
		for i in range(self.num):
			self.types[i] = struct.unpack("!L",message[2+4*i,2+4*(i+1)])
class ParameterStatus(StringPair): pass

@frontend('P')
def Parse(dest, query, types):
	return dest.encode('utf-8')+b'\0' + query.encode('utf-8')+b'\0'+encode_ints(types,4)

class ParseComplete(Message): pass

@frontend('p')
def PasswordMessage(password):
	return password.encode('utf-8')+b'\0'

class PortalSuspended(Message): pass

@frontend('Q')
def Query(query):
	return query.encode('utf-8')+b'\0'

class ReadyForQuery(Message):
	idle = False
	in_transaction = False
	failed = False
	def __init__(self,message):
		self.status = message[0].decode()
		if self.status == 'I':
			self.idle = True
		elif self.status == 'T':
			self.in_transaction = True
		elif self.status == 'E':
			self.failed = True

class RowDescription(Message):
	def __init__(self, message):
		self.num = struct.unpack("!H",message[:2])
		self.fields = [None]*self.num
		for i in range(self.num):
			field = Field()
			field.name, message = message.split(0,1)
			field.oid, field.typlen, field.typmod = struct.unpack("!LHL",message)
			message = message[4+2+4:]
			self.fields[i] = field

def SSLRequest():
	mess = struct.pack("!l",80877103)
	return struct.pack("!l",len(mess))+mess
			
def StartupMessage(name,value):
	mess = struct.pack("!l",196608) + name.encode('utf-8')+b'\0'+value.encode('utf-8')+b'\0'
	return struct.pack("!l",len(mess))+mess

@frontend('S')
def Sync(): pass

@frontend('X')
def Terminate(): pass
			
# read these FROM the server
backend = {
	'R': Authentication,
	'K': BackendKeyData,
	'2': BindComplete,
	'3': CloseComplete,
	'C': CommandComplete,
	'd': CopyData,
	'c': CopyDone,
	'f': CopyFail,
	'G': CopyInResponse,
	'H': CopyOutResponse,
	'W': CopyBothResponse,
	'D': DataRow,
	'l': EmptyQuery,
	'E': Error,
	'V': FunctionCallResponse,
	'n': NoData,
	'N': NoticeResponse,
	'A': NotificationResponse,
	't': ParameterDescription,
	'S': ParameterStatus,
	'1': ParseComplete,
	's': PortalSuspended,
	'Z': ReadyForQuery,
	'T': RowDescription,
}

# send these TO the server
frontend = {
	'B': Bind,
	'C': Close,
	'd': CopyData,
	'c': CopyDone,
	'f': CopyFail,
	'D': Describe,
	'E': Execute,
	'H': Flush,
	'F': FunctionCall,
	'P': Parse,
	'p': PasswordMessage,
	'Q': Query,
	'S': Sync,
	'X': Terminate
}

# make sure bytes
def arrayify(derp):
	derp = [(k.encode(),v) for k,v in derp.items()]
	barr = [0]*max(derp[0] for derp in derp)
	for k,v in derp:
		barr[k] = v
	return barr
backend = arrayify(backend)

def read_messages(inp):
	while True:
		typ,message = raw_read(inp)
		backend[typ](message).dispatch(inp)
