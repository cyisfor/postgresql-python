# these are the things you can send to the server (i.e. "frontend" messages)
# each is a function, that sends stuff to the underlying db socket when called.

from flatten import flatten
from . import main

import struct

def P(fmt,*stuff):
	return struct.pack('!'+fmt,*stuff)

def send(typ,*contents):
	contents = tuple(flatten(contents))
	total = sum(len(thing) for thing in contents)
	# have to measure the total size to start the message
	# but can send the data piecewise if it's too big
	buf = [typ + P("L",total+4)]
	cur = len(buf[0])
	for thing in contents:
		buf.append(thing)
		cur += len(thing)
		if cur > 0x1000:
			main.c.send(b''.join(buf))
			buf = []

def int_array(elems, sizefmt='h', elemfmt='h'):
	return P(sizefmt + (elemfmt * len(elems)),*elems)
	
def String(s):
	return s.encode('utf-8') + b'\0'

def Blob(thing,fmt='i'):
	return P(fmt,len(thing))+thing

def Blobs(things,fmt='h',elemfmt='i'):
	return chain((P(fmt,len(things)),),
							 (Blob(thing,elemfmt) for thing in things))

def Bind(dest, source, formats, values, result_formats):
	send
	(b'B',String(dest), String(source),
	 int_array(formats),
	 Blobs(values),
	 int_array(result_formats))

def CancelRequest(target,secret):
	# it's always 16 bytes long, so don't need to pack twice
	return P("iiii",16,80877102,target,secret)

def Close(is_portal, name):
	send(b'C', b'P' if is_portal else b'S' + String(name))

def CopyData(data):
	send(b'd',data)

def CopyDone():
	send(b'c')

def CopyFail(error):
	send(b'f',String(error))

def Describe(is_portal, name):
	send(b'D',b'P' if is_portal else b'S' + String(name))
		
def Execute(name, max_rows=0):
	send(b'E',String(name)+struct.pack('!L',max_rows))

def Flush():
	send(b'H')

def FunctionCall(oid, formats, arguments, result_format):
	send(b'F', P("i",oid), int_array(formats),
			 Blobs(arguments),
			 P("h",result_format))

def PasswordMessage(password):
	send(b'p',String(password))

def Query(query):
	send(b'Q',String(query))

def SSLRequest():
	main.c.send(struct.pack("!ii",8,80877103))
			
def StartupMessage(name,value):
	mess = struct.pack("!i",196608) + String(name)+String(value)
	return struct.pack("!i",len(mess))+mess

def Sync():
	send(b'S')

def Terminate():
	send(b'X')
