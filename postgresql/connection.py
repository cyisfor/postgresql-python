from . import interface,arrayparser
from itertools import count,islice
import datetime
import traceback
import sys,time

# TODO: don't hard code this
# note: gevent can monkey patch this so greenlets work.
import select

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
	def __getitem__(self,n):
		return self.info[n]

E = interface.ExecStatus
OKstatuses = set((
	E.COMMAND_OK,
	E.TUPLES_OK,
	E.COPY_OUT,
	E.COPY_IN,
	E.COPY_BOTH,
	E.SINGLE_TUPLE))

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

def oneresult(results):
	result = next(results)
	try:
		res = next(results)
		print("warning: not just one result",res.statusId)
	except StopIteration: pass
	return result

def notReentrant(f):
	def wrapper(self,*a,**kw):
		#print("busy",id(self),a)
		assert not self.safe.busy, (self.safe.busyb, (f,a,kw))
		self.safe.busy = True
		self.safe.busyb = (f,a,kw)
		try:
			g = f(self,*a,**kw)
			#print(a)
			#print("ret",g)
			if(hasattr(g,'__next__')):
				return tuple(g)
			return g
		finally:
			self.safe.busy = False
			self.safe.busyb = None
			#print("nabusy",id(self),a)
	return wrapper

def parseNumber(result):
	if result == 'NULL':
		return None
	if '.' in result:
		return float(result)
	return int(result)

def getError(raw):
	error = {}
	derp = interface.connectionErrorMessage(raw)

	for k,v in (('message',interface.errorMessage(raw)),
							('connection',derp),
							('severity',interface.errorField(raw,interface.PG.DIAG_SEVERITY)),
							('primary',interface.errorField(raw,interface.PG.DIAG_MESSAGE_PRIMARY)),
							('detail',interface.errorField(raw,interface.PG.DIAG_MESSAGE_DETAIL)),
							('hint',interface.errorField(raw,interface.PG.DIAG_MESSAGE_HINT)),
							('context',interface.errorField(raw,interface.PG.DIAG_CONTEXT)),
							('internal', interface.errorField(raw,interface.PG.DIAG_INTERNAL_QUERY))):
		error[k] = v
		position = interface.errorField(raw,interface.PG.DIAG_STATEMENT_POSITION)
		if position:
			error['position'] = (
				int(position),
				int(interface.errorField(raw,interface.PG.DIAG_INTERNAL_POSITION)))
		function = interface.errorField(raw,interface.PG.DIAG_SOURCE_FUNCTION)
		if function:
			error['function'] = {
				'location': interface.errorField(raw,interface.PG.DIAG_SOURCE_FILE),
				'line': interface.errorField(raw,interface.PG.DIAG_SOURCE_FILE),
				'name': function,
			}
	return error

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
		if self.statusId in {E.COPY_OUT,E.COPY_IN}:
			interface.freeResult(raw)
			return
		resStatus = interface.resStatus(self.statusId)
		if resStatus:
			self.status = resStatus
		if self.statusId not in OKstatuses:
			error = getError(rawconn)
			interface.freeResult(raw)
			if self.statusId != E.NONFATAL_ERROR:
				if self.verbose:
					sys.stderr.write('\n'.join(repr(s) for s in (
						stmt,
						repr(args),
						self.status,
						error)))
					sys.stderr.flush()
				raise SQLError(stmt,error)
			else:
				self.error = error
			return
		self.tuplesUpdated = interface.tuplesUpdated(raw)
		if self.tuplesUpdated:
			self.tuplesUpdated = int(self.tuplesUpdated)

		self.fields = []
		self.types = []
		for c in range(interface.nfields(raw)):
			fname = interface.fname(raw,c)
			if fname is None:
				self.fields.append(None)
			else:
				self.fields.append(self.decode(ctypes.string_at(fname)))
			ftype = interface.ftype(raw,c)
			if ftype is None:
				self.types.append(None)
			else:
				self.types.append(int(ftype))
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
		interface.freeResult(raw)

stmtcounter = count(0)

def anonstatement():
	return 'anonstatement{}'.format(next(stmtcounter))

import ctypes
def cstrize(s):
	b = s.encode('utf-8')
	return ctypes.c_char_p(b)

def makederp(typ,args):
	if not args: return None
	typ *= len(args)
	val = typ(*args)
	return val

class LocalConn(threading.local):
	raw = None
	canceller = None
	busy = False
	poll = None
	busyb = None

def pollout(f):
	def wrapper(self,raw,*a,**kw):
		sock = interface.socket(raw)
#		print(sock,raw)
		self.safe.poll.modify(sock,select.POLLOUT)
		try:
			return f(self,raw,*a,**kw)
		finally:
			self.safe.poll.modify(sock,select.POLLIN)
	return wrapper


def consume(raw):
	interface.consume(raw)
	while True:
		notify = interface.notifies(raw)
		if not notify: break
		notify = notify.contents
		print("notify",notify.name,notify.pid,notify.extra) # meh!

quotes = set((b"'"[0],b'"'[0]))

class Canceller:
	raw = None
	def __del__(self):
		raw = self.raw
		self.raw = None
		if raw is not None:
			interface.freeCancel(raw)
	def __init__(self,conn):
		self.raw = interface.canceller(conn.safe.raw)
	def cancel(self):
		s = ctypes.create_string_buffer(0x1000)
		ret = interface.cancel(self.raw, s, 0x1000)
		if ret == 0:
			raise SQLError("Could not cancel: "+s)

class Connection:
	inTransaction = False
	savePoint = None
	verbose = False
	out = None
	def checkOne(self,i):
		if i != 1:
			raise SQLError("derp",getError(self.safe.raw))
	def __init__(self,**params):
		if 'params' in params:
			params.update(params['params'])
			del params['params']
		self._ctypessuck = (" ".join(n+"="+repr(v) for n,v in params.items())).encode("utf-8")
		self.params = ctypes.create_string_buffer(self._ctypessuck)
		self.safe = LocalConn()
		self.executedBefore = set()
		self.prepareds = dict()
	specialDecoders = None
	stringOIDs = ()
	def reconnecting(self,f):
		while True:
			try:
				return f()
			except SQLError as e:
				if e['connection'] and e['connection'].startswith(
						b'server closed the connection unexpectedly'):
					self.reconnect()
				else: raise
	def results(self, raw, stmt, args):
#		print("start results",stmt)
		try:
			yield from self.derp_cancellable_results(raw, stmt, args)
		except SQLError:
			raise
		except GeneratorExit as e:
			raise
		except:
			self.safe.canceller.cancel()
			print("Requested cancel...",sys.exc_info())
			self.poll(1000) # wait a bit to give it a chance?
			raise
#		finally:
#			print("end results",stmt)
#			interface.next(raw) # why an extra one?
	def derp_cancellable_results(self,raw,stmt,args=()):
		consume(raw)
		i=0
		oldresult = None
		while True:
			i += 1
			while interface.isBusy(raw):
				while True:
					res = self.poll(1000)
					if len(res) > 0: break
					assert len(res) == 0
					print("Waiting on",stmt,res)
				consume(raw)
			result = interface.next(raw)
			if not result:
				return
			result = Result(self,raw,result,stmt,args)
			self.status = result.statusId
			yield result
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
	def poll(self, t=None):
		return self.safe.poll.poll(t)
	def connect(self):
		need_setup = False
		if self.safe.raw is None:
			self.establish_connection()
			need_setup = True
		self.reconnect()
		if need_setup:
			# but don't setup if we already did!
			interface.setErrorVerbosity(self.safe.raw,interface.Verbosity.VERBOSE)
			self.safe.canceller = Canceller(self)
			self.setup(self.safe.raw)
		return self.safe.raw

	def establish_connection(self):
		P = interface.PollingStatus
		delay = 0.1
		while True:
			raw = interface.connect(self.params,1)
			status = interface.status(raw)
			if status == interface.ConnStatus.BAD:
				print("bad connection...")
				sleep(delay)
				delay *= 1.5
				continue
			self.safe.poll = select.poll()
			sock = interface.socket(raw)
			self.safe.poll.register(sock, select.POLLIN)
			delay = 0.1
			while True:
				res = interface.connectPoll(raw)
				print("connecting...",res,interface.status(raw))
				if res == P.OK:
					self.safe.poll.modify(sock,select.POLLIN)
					self.safe.raw = raw
					# this is the only place to return.
					return
				elif res == P.READING:
					self.safe.poll.modify(sock,select.POLLIN)
				elif res == P.WRITING:
					self.safe.poll.modify(sock,select.POLLOUT)
				else:
					self.safe.poll.modify(sock,select.POLLIN)
					print("Polling status failed!")
					sleep(1)
					break
				delay = 1
				while True:
					# SIGH
					events = self.poll(1000 * delay)
					if events:
						break
					print("poll timeout on connecting...",res,delay)
					delay *= 1.5
		raise RuntimeError("never get here!")

	def reconnect(self):
		boop = False
		raw = self.safe.raw
		stat = interface.status(raw)
		C = interface.ConnStatus
		if stat != C.OK:
			if stat == C.BAD:
				boop = True
				print("connection bad?",interface.status(raw),getError(raw))
				import time
				time.sleep(1)
				interface.reset(raw)
			while not interface.status(raw) not in {C.OK,C.MADE}:
				self.poll(1000)
				consume(raw)
				interface.connectPoll(raw)
		if boop:
			self.executedBefore = set()
			self.prepareds = dict()
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
	busy = False
	@notReentrant
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
#				print(stmt % derpsubs)
			stmt = stmt % subs
			args = [args[key] for key in keys]
			# badda boom
		args = [self.encode(arg) for arg in args]
		values = makederp(ctypes.c_char_p,args)
		lengths = makederp(ctypes.c_int,[len(arg) for arg in args])
		fmt = makederp(ctypes.c_int,(0,)*len(args))
		if self.verbose:
			out = self.out if self.out else sys.stdout
			out.write(str(time.time())+' '+stmt)
			out.write(' '.join((':',)+tuple(map(repr,args)))+'\n')
			out.flush()
		@self.reconnecting
		def _():
			name = self.prepareds.get(stmt)
			if name is None:
				types = None
				name = anonstatement()
				def reallyprepare():
#					print("Prepare",stmt)
					self.checkOne(interface.send.prepare(
						raw,
						name.encode('utf-8'),
						stmt.encode('utf-8'),
						len(args),
						types))
					while True:
						result = oneresult(self.results(raw,stmt,args))
						if not result: break
#						print(result)
#					print("Prepare done",stmt)
					# this is only the result of PREPARATION not executing it
					prep = Prepared(name)
					self.prepareds[stmt] = prep
					return prep
				if hash(stmt) in self.executedBefore:
					# prepare if it's executed twice, otherwise don't bother
					while True:
						try:
							name = reallyprepare()
							break
						except SQLError as e:
							if ('prepared statement "'+name+'" already exists').encode() in e.info['connection']:
								print(name,'already exists. retrying creating the prepared statement...')
								time.sleep(1)
							else: raise
				else:
#					print("Noprep1",stmt)
					self.checkOne(interface.send.noprep.query(
						raw,
						stmt.encode('utf-8'),
						len(args) if args else 0,
						types,
						values,
						lengths,
						fmt,
						0))
					self.result = oneresult(self.results(raw,stmt,args))
#					print("Noprep1 done",stmt)
					if len(stmt) > 14: # don't bother preparing short statements ever
						self.executedBefore.add(hash(stmt))
					return self.result
			try:
#				print("Query",stmt)
				self.checkOne(interface.send.query(
					raw,
					name.encode('utf-8'),
					len(args),
					values,
					lengths,
					fmt,
					0))
				self.result = oneresult(self.results(raw,stmt,args))
#				print("Query done",stmt)
			except SQLError as e:
				print("ummmm",e)
				print(dir(e))
				print(self.status)
				raise
		if self.verbose:
			self.out.write(str(self.result))
		return self.result
	@notReentrant
	def copy(self,stmt,source=None, args=()):
		@self.reconnecting
		def gen():
			nonlocal source
			raw = self.connect()
#			print("Noprep",stmt)
			self.checkOne(interface.send.noprep.query(
				raw,
				stmt.encode('utf-8'),
				0,
				None,
				None,
				None,
				None,
				0))
			for result in self.results(raw,stmt,args):
				if result.statusId in {E.COPY_OUT,E.COPY_IN}:
					break
				else:
					print("um... no copy?")
					return
#			print("Noprep done",stmt)
			if result.statusId == E.COPY_OUT:
				yield from self.copyTo(raw,stmt,args)
			else:
				oldsource = None
				oldoff = 0
				def bytessource(buf):
					nonlocal oldsource
					buf[:] = oldsource[oldoff:]
					oldoff += max(len(buf),len(oldsource)-oldoff)
				if isinstance(source,str):
					oldsource = source.encode('utf-8')
					source = bytessource
				elif isinstance(source,(bytes,bytearray)):
					source = bytessource
				elif isinstance(source,memoryview):
					oldsource = source.cast('B')
					source = bytessource
				else:
					if hasattr(source,'read'):
						if hasattr(source,'buffer'):
							source = source.buffer
					if hasattr(source,'readinto'):
						source = source.readinto
					else:
						oldsource = source
						def source(buf):
							s = oldsource.read(len(buf))
							if hasattr(s,'encode'):
								s = s.encode('utf-8')
							buf[:] = s
							return len(buf)
				return self.copyFrom(raw,stmt,args,source)
		return gen
	def copyTo(self,raw,stmt,args):
		buf = ctypes.c_char_p(None)
		while True:
			code = interface.getCopyData(raw,ctypes.byref(buf),1)
			if code == 0:
				self.poll()
				consume(raw)
				continue
			elif code == -1:
				break
			elif code == -2:
				raise SQLError(stmt,getError(raw))
			else:
				def row():
					row = ctypes.string_at(buf,code-1) # ignore \n
					# if text mode...
					row = row.split(b'\t')
					for i,val in enumerate(row):
						yield self.demogrify(val,stmt.types[i])
				yield tuple(row())
		# copy TO returns 1 result before (endlessly) and 1 result after (w/ NULL)
		self.result = oneresult(self.results(raw,stmt,args))
		return self.result
	@pollout
	def copyFrom(self,raw,stmt,args,source):
		def putAll():
			buf = bytearray(0x1000)
			while True:
				amt = source(buf)
				if not amt:
					break
#				print("copying from",amt)
				while True:
					arr = ctypes.c_char * amt
					res = interface.putCopyData(raw,arr.from_buffer(buf),amt)
					if res == 0:
						self.poll()
					elif res == 1:
						break
					else:
						raise SQLError(stmt,getError(raw))
		def putEnd(error=None):
			while self.flush(raw): pass
			while True:
				res = interface.putCopyEnd(raw,error)
				if res == 0:
					self.poll()
				elif res == 1:
					res = self.flush(raw)
					if res == -1:
						raise SQLError(stmt,getError(raw))
					if res == 0:
						return
					# if res == 0: continue since putCopyEnd overflowed and was dropped
				else:
					raise SQLError(stmt,getError(raw))
		try:
			putAll()
		except Exception as e:
			putEnd(str(e).encode('utf-8'))
		else:
			putEnd()
		self.result = oneresult(self.results(raw,stmt,args))
		return self.result
	def flush(self,raw):
		self.poll()
		return interface.flush(raw)
