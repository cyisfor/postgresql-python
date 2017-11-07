from . import connection

SQLError = connection.SQLError

from contextlib import contextmanager
from itertools import count
from weakref import WeakSet

import random

savectr = count(0)

class Connection(connection.Connection):
	def __init__(self,*a,**kw):
		ret = super().__init__(*a,**kw)
		self.safe.cursors = WeakSet()
		return ret
	def close_cursors(self):
		for cursor in self.safe.cursors:
			cursor.closed = True # automatically closed by transaction ending
	def retransaction(self,rollback=False):
		if not self.inTransaction: return
#		print("RETRANSACTION",rollback)
		if rollback:
			self.close_cursors()
			self.execute('ROLLBACK')
		else:
			self.close_cursors()
			self.execute('COMMIT')
		self.execute('BEGIN')
		
	def begin(self):
		"please don't use this if possible..."
		self.inTransaction = True
		self.execute("BEGIN")
#		print("BEGIN")

	def rollback(self):
#		print("ROLLBACK")
		self.close_cursors()
		self.execute("ROLLBACK")
		self.inTransaction = False

	def commit(self):
#		print("COMMIT")
		self.close_cursors()
		self.execute("COMMIT")
		self.inTransaction = False

	@contextmanager
	def transaction(self):
		if self.inTransaction:
			# Not allowing nested transactions... because too long a transaction makes db slo-o-o-ow.
			# This totally breaks the "with" syntax as a transaction should last to the end of it.
			# But it's easier to just be in a transaction and commit periodically at known good spots.
			# One such commit in code that's considered a bad spot is not a db problem, but badly
			# written code.
			self.retransaction()
			yield self
			return
		self.begin()
		try:
			yield self
		except:
			if self.verbose:
				import traceback
				traceback.print_exc()
			self.rollback()
			raise
		self.commit()

	@contextmanager
	def saved(self):
		name = "savepoint{}".format(next(savectr))
		self.execute("SAVEPOINT "+name)
		try:
			yield self
			self.execute("RELEASE SAVEPOINT "+name)
		except Exception as ee:
			try: self.execute("ROLLBACK TO "+name)
			except SQLError as e:
				print(e)
				print('-'*60)
				print(ee)
			raise(ee)


	def cursor(self,name,sql,args=()):
		# cursors may ONLY execute in transactions, but they're useful to hold onto...
		# for scrolling forward and backward through results.
		# can't commit the transaction though, because it'll pull EVERY result, taking forever
		return Cursor(self,name,sql,args)

class Cursor:
	offset = 0
	args = ()
	closed = True
	def __hash__(self):
		return hash(self.sql)
	def __eq__(self,other):
		return self.sql == other.sql
	def __init__(self,connection,name,sql,args):
		self.args = args
		self.connection = connection
		self.name = name
		self.sql = sql
		self.open()
	def open(self):
		if not self.closed: return
		self.closed = False
		if not self.connection.inTransaction:
			self.connection.begin()
#		print("DECLARE",self.name,self.connection.raw)
		self.connection.execute("DECLARE " +
														self.name + " SCROLL CURSOR WITHOUT HOLD FOR " +
														self.sql,
														self.args)
		self.connection.safe.cursors.add(self)
		if self.offset:
			self.connection.execute("MOVE ABSOLUTE " +str(self.offset)+" FROM " + self.name)
	def close(self):
		if self.closed: return
#		print("CLOSE",self.name,self.connection.raw)
		self.connection.execute("CLOSE "+self.name)
		self.connection.safe.cursors.remove(self)
		self.closed = True
		if not self.connection.safe.cursors:
			# you can't write to the database with cursors outstanding
			# b/c too expensive to end the transaction with a hold cursor
			# so if no more cursors, give waiting writes a chance to go
			self.connection.retransaction()

	def __enter__(self):
		self.open()
	def __exit__(self,type,value,traceback):
		self.close()
	def __del__(self):
		print("del",self.name)
		try: self.close()
		except Exception as e:
			import traceback
			traceback.print_exc()
	def same(self,sql,args):
		if sql == self.sql:
			if args == self.args:
				return True
			self.args = args
		else:
			self.sql = sql
			self.args = args
		return False
	def move(self,start):
		if start == self.offset: return
		self.open()
		diff = start - self.offset
		self.connection.execute("MOVE RELATIVE " +str(diff)+" FROM " + self.name)
		self.offset = start
	def fetch(self,limit):
		self.open()
		self.offset += limit
		return self.connection.execute("FETCH FORWARD "+str(limit)+" FROM " + self.name)

