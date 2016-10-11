from . import interface,arrayparser
from itertools import count,islice
import datetime
import traceback
import time

import sys

import threading

class Prepared(str): pass

class SQLError(IOError):
	def __init__(self,stmt,info):
		self.stmt = stmt
		self.info = info
	def __str__(self):
		return self.stmt+'\n\n'+'\n'.join((k+'='+str(v) for k,v in self.info.items()))
	def __repr__(self):
		return 'SQLError('+repr(self.info)+')'

OKstatuses = set((
	interface.PGRES_COMMAND_OK,
	interface.PGRES_TUPLES_OK,
	interface.PGRES_COPY_OUT,
	interface.PGRES_COPY_IN,
	interface.PGRES_COPY_BOTH,
	interface.PGRES_SINGLE_TUPLE))

def parseDate(result):
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

def parseNumber(result):
	if result == 'NULL':
		return None
	if '.' in result:
		return float(result)
	return int(result)

class Result(list):
	error = None
	tuplesUpdated = None
	status = None
	verbose = None
	def __init__(self,conn,rawconn,raw,stmt,args):
		# no reason to leave the result sitting around in C structures now?
		self.verbose = conn.verbose
		self.decode = conn.decode
		self.demogrify = conn.demogrify
		self.statusId = interface.resultStatus(raw)
		resStatus = interface.resStatus(self.statusId)
		if resStatus:
			self.status = resStatus
		if self.statusId not in OKstatuses:
			error = {}
			derp = interface.connectionErrorMessage(rawconn)
			print("DERP",derp)
			for k,v in (('message',interface.errorMessage(raw)),
									('connection',derp),
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
			if self.statusId != interface.PGRES_NONFATAL_ERROR:
				if self.verbose:
					sys.stderr.write('\n'.join(
						stmt,
						repr(args),
						self.status,
						error))
					sys.stderr.flush()

				raise SQLError(stmt,error)
			else:
				self.error = error
		else:
			self.tuplesUpdated = interface.tuplesUpdated(raw)
			if self.tuplesUpdated:
				self.tuplesUpdated = int(self.tuplesUpdated)
		self.fields = []
		self.types = []
		for c in range(interface.nfields(raw)):
			self.fields.append(self.decode(ctypes.string_at(interface.fname(raw,c))))
			self.types.append(int(interface.ftype(raw,c)))
		for r in range(interface.ntuples(raw)):
			row = list()
			for c in range(interface.nfields(raw)):
				if interface.getisnull(raw,r,c):
					val = None
				else:
					length = interface.getlength(raw,r,c)
					rawval = interface.getvalue(raw,r,c)
					val = self.demogrify(rawval,self.types[c])
				row.append(val)
			self.append(row)
		interface.clear(raw)

stmtcounter = count(0)

def anonstatement():
	return 'anonstatement{}'.format(next(stmtcounter))

import ctypes
def cstrize(s):
	return ctypes.c_char_p(s.encode('utf-8'))

def makederp(typ,args):
	if not args: return None
	typ *= len(args)
	val = typ(*args)
	return val

class LocalConn(threading.local):
	raw = None

quotes = set((b"'"[0],b'"'[0]))

class Connection:
	inTransaction = False
	savePoint = None
	verbose = False
	out = None
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
		self.conninfo = (keya,vala,1)
		self.safe = LocalConn()
		self.executedBefore = set()
		self.prepareds = dict()
	specialDecoders = None
	stringOIDs = ()
	def setup(self,raw):
		if self.specialDecoders: return
		self.specialDecoders = {}
		for oid in self.getOIDs(raw,'N'):
			self.specialDecoders[oid] = self.decodeNumber
		for oid in self.getOIDs(raw,'B'):
			self.specialDecoders[oid] = self.decodeBoolean
		for oid in self.getOIDs(raw,'D'):
			self.specialDecoders[oid] = self.decodeDate
		self.stringOIDs = set(self.getOIDs(raw,'S'))
		for oid in self.stringOIDs:
			self.specialDecoders[oid] = self.decode
		for oid,subtype in self.executeRaw(raw,"SELECT typarray,oid FROM pg_type WHERE typarray > 0"):
			parser = self.makeParseArray(subtype)
			if parser:
				self.specialDecoders[oid] = parser
	def registerDecoder(self,decoder,name,namespace='public'):
		raw = self.connect()
		nsnum = self.executeRaw(raw, 'SELECT oid FROM pg_namespace WHERE nspname = $1::name',(namespace,))
		assert nsnum;
		nsnum = nsnum[0][0]
		oid = self.executeRaw(raw,'SELECT oid FROM pg_type WHERE typnamespace = $1::oid AND typname = $2::name',(nsnum,name))
		assert oid
		oid = oid[0][0]
		self.specialDecoders[oid] = decoder
		print('registered decoder for ',namespace+'.'+name,oid)
	def getOIDs(self,raw,category):
		return (int(row[0]) for row in self.executeRaw(raw,"SELECT oid FROM pg_type WHERE typcategory = $1",(category,)))
	def decodeString(self, s):
		# this is a huge copout
		if s and s[0] in quotes:
			s = s[1:-1]
		return self.decode(s)
	def decodeNumber(self, s):
		return parseNumber(self.decode(s))
	def decodeDate(self, s):
		return parseDate(self.decode(s))
	def decodeBoolean(self, b):
		return b == b't'
	def makeParseArray(self,subtype):
		if subtype in self.stringOIDs:
			decoder = self.decodeString
		else:
			decoder = self.specialDecoders.get(subtype)
		if decoder:
			return lambda result: arrayparser.decode(result,decoder)
	def decode(self,b):
		return b.decode('utf-8',errors='replace')
	def demogrify(self,result,typ):
		decoder = self.specialDecoders.get(typ)
		if decoder:
			return decoder(result)
		return result
	def connect(self):
		need_setup = False
		if self.safe.raw is None:
			self.safe.raw = interface.connect(*self.conninfo)
			# can't setup until we have a good connection...
			need_setup = True
		while interface.status(self.safe.raw) != interface.CONNECTION_OK:
			print("connection bad?")
			import time
			time.sleep(1)
			interface.reset(self.safe.raw)
		if need_setup:
			# but don't setup if we already did!
			interface.setErrorVerbosity(self.safe.raw,interface.PQERRORS_VERBOSE)
			self.setup(self.safe.raw)
		return self.safe.raw
	def mogrify(self,i):
		if i is None:
			return 'NULL'
		if isinstance(i,int) or isinstance(i,float):
			return str(i)
		elif isinstance(i,bytes):
			return self.decode(i)
		elif isinstance(i,str):
			return i
		elif hasattr(i,'asPostgreSQL'):
			return i.asPostgreSQL(self)
		elif isinstance(i,datetime.datetime) or isinstance(i,datetime.date) or isinstance(i,datetime.time):
			return i.isoformat()
		elif isinstance(i,list) or isinstance(i,tuple) or isinstance(i,set):
			return '{'+','.join(self.mogrify(ii) for ii in i)+'}'
		elif hasattr(i,'__next__'):
			# XXX: infinite iterators hang + 100% CPU + 100% disk usage + no ^C etc
			# and we can't check for them because Halting Problem
			# so... 1000 is a good enough maximum size right?
			return self.mogrify(tuple(islice(i,1000)))
		else:
			raise RuntimeError("Don't know how to mogrify type {}".format(type(i)))
	def encode(self,i):
		return self.mogrify(i).encode('utf-8')
	def execute(self,stmt,args=()):
		return self.executeRaw(self.connect(),stmt,args)
	def executeRaw(self,raw,stmt,args=()):
		if isinstance(args,dict):
			# just figured out a neat trick to let %(named)s parameters
			keys = args.keys()
			for key in keys:
				if not '%('+key+')' in stmt:
					raise SQLError(stmt,{'message': "Named args must all be in the statement! Missing "+key})
			subs = dict(zip(keys,['$'+str(i) for i in range(1,1+len(keys))]))
			if self.verbose:
				def derpify(s):
					if isinstance(s,int):
						return str(s)
					else:
						return "'"+str(s)+"'"
				derpsubs = dict((key,derpify(args[key])) for key in keys)
				print(stmt % derpsubs)
			stmt = stmt % subs
			args = [args[key] for key in keys]
			# badda boom
		args = [self.encode(arg) for arg in args]
		values = makederp(ctypes.c_char_p,args)
		lengths = makederp(ctypes.c_int,[len(arg) for arg in args])
		fmt = makederp(ctypes.c_int,(0,)*len(args))
		fullstmt = stmt
		if self.verbose:
			import sys,time
			out = self.out if self.out else sys.stdout
			out.write(str(time.time())+' '+fullstmt)
			out.write(' '.join((':',)+args)+'\n')
			out.flush()
		name = self.prepareds.get(stmt)
		if name is None:
			types = makederp(ctypes.c_void_p,(None,)*len(args))
			if not stmt in self.executedBefore:
				name = anonstatement()
				result = interface.prepare(raw,
						name.encode('utf-8'),
						stmt.encode('utf-8'),
						len(args),
						types)
				Result(self,raw,result,fullstmt,args) # needed to catch/format errors
				self.prepareds[stmt] = Prepared(name)
			else:
				result = interface.executeOnce(raw,
						stmt.encode('utf-8'),
						len(args) if args else 0,
						types,
						values,
						lengths,
						fmt,
						0);
				self.status = interface.resultStatus(result)
				self.result = Result(self,raw,result,fullstmt,args)
				if len(stmt) > 14:
					self.executedBefore.add(stmt)
				return self.result

		result = interface.execute(raw,
				name.encode('utf-8'),
				len(args),
				values,
				lengths,
				fmt,
				0)
		self.status = interface.resultStatus(result)
		self.result = Result(self,raw,result,fullstmt,args)
		if self.verbose:
			self.out.write(str(self.result))
		return self.result
	def copy(self,stmt,source=None):
		raw = self.connect()
		result = interface.executeOnce(raw,
				stmt.encode('utf-8'),
				0,
				None,
				None,
				None,
				None,
				0)
		self.status = interface.resultStatus(result)
		if 'TO' in stmt:
			return self.copyIn(stmt,raw)
		else:
			return self.copyOut(source,raw)
	def copyIn(self,stmt,raw):
		while True:
			buf = ctypes.c_char_p(None)
			code = interface.getCopyData(raw,ctypes.byref(buf),0)
			if code == -1:
				return
			elif code == -2:
				message = self.decode(interface.connectionErrorMessage(raw))
				if self.verbose:
					out = self.out if self.out else sys.stdout
					out.write(message+'\n')
					out.flush()
				raise SQLError(stmt,{'message': message})
			else:
				yield self.decode(ctypes.string_at(buf))
	def copyOut(self,source,raw):
		try:
			while True:
				buf = source.read(0x1000)
				if not buf: break
				if isinstance(buf,str):
					buf = buf.encode('utf-8')
				interface.putCopyData(raw,buf,len(buf))
		except Exception as e:
			interface.putCopyEnd(raw,str(e).encode('utf-8'))
			raise
		interface.putCopyEnd(raw,None)
