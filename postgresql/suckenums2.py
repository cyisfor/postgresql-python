from ctypes import c_int
class ConnStatus(c_int):
	def __hash__(self):
		return self.value
	def __eq__(self,other):
		return self.value == other
	def __str__(self):
		val = self.value
		if val == ConnStatus.AUTH_OK:
			return 'ConnStatus.AUTH_OK(5)'
		if val == ConnStatus.AWAITING_RESPONSE:
			return 'ConnStatus.AWAITING_RESPONSE(4)'
		if val == ConnStatus.BAD:
			return 'ConnStatus.BAD(1)'
		if val == ConnStatus.MADE:
			return 'ConnStatus.MADE(3)'
		if val == ConnStatus.NEEDED:
			return 'ConnStatus.NEEDED(8)'
		if val == ConnStatus.OK:
			return 'ConnStatus.OK(0)'
		if val == ConnStatus.SETENV:
			return 'ConnStatus.SETENV(6)'
		if val == ConnStatus.SSL_STARTUP:
			return 'ConnStatus.SSL_STARTUP(7)'
		if val == ConnStatus.STARTED:
			return 'ConnStatus.STARTED(2)'
	AUTH_OK = 5
	AWAITING_RESPONSE = 4
	BAD = 1
	MADE = 3
	NEEDED = 8
	OK = 0
	SETENV = 6
	SSL_STARTUP = 7
	STARTED = 2
class ContextVisibility(c_int):
	def __hash__(self):
		return self.value
	def __eq__(self,other):
		return self.value == other
	def __str__(self):
		val = self.value
		if val == ContextVisibility.CONTEXT_ALWAYS:
			return 'ContextVisibility.CONTEXT_ALWAYS(2)'
		if val == ContextVisibility.CONTEXT_ERRORS:
			return 'ContextVisibility.CONTEXT_ERRORS(1)'
		if val == ContextVisibility.CONTEXT_NEVER:
			return 'ContextVisibility.CONTEXT_NEVER(0)'
	CONTEXT_ALWAYS = 2
	CONTEXT_ERRORS = 1
	CONTEXT_NEVER = 0
class ExecStatus(c_int):
	def __hash__(self):
		return self.value
	def __eq__(self,other):
		return self.value == other
	def __str__(self):
		val = self.value
		if val == ExecStatus.BAD_RESPONSE:
			return 'ExecStatus.BAD_RESPONSE(5)'
		if val == ExecStatus.COMMAND_OK:
			return 'ExecStatus.COMMAND_OK(1)'
		if val == ExecStatus.COPY_BOTH:
			return 'ExecStatus.COPY_BOTH(8)'
		if val == ExecStatus.COPY_IN:
			return 'ExecStatus.COPY_IN(4)'
		if val == ExecStatus.COPY_OUT:
			return 'ExecStatus.COPY_OUT(3)'
		if val == ExecStatus.EMPTY_QUERY :
			return 'ExecStatus.EMPTY_QUERY (0)'
		if val == ExecStatus.FATAL_ERROR:
			return 'ExecStatus.FATAL_ERROR(7)'
		if val == ExecStatus.NONFATAL_ERROR:
			return 'ExecStatus.NONFATAL_ERROR(6)'
		if val == ExecStatus.SINGLE_TUPLE:
			return 'ExecStatus.SINGLE_TUPLE(9)'
		if val == ExecStatus.TUPLES_OK:
			return 'ExecStatus.TUPLES_OK(2)'
	BAD_RESPONSE = 5
	COMMAND_OK = 1
	COPY_BOTH = 8
	COPY_IN = 4
	COPY_OUT = 3
	EMPTY_QUERY  = 0
	FATAL_ERROR = 7
	NONFATAL_ERROR = 6
	SINGLE_TUPLE = 9
	TUPLES_OK = 2
class Ping(c_int):
	def __hash__(self):
		return self.value
	def __eq__(self,other):
		return self.value == other
	def __str__(self):
		val = self.value
		if val == Ping.NO_ATTEMPT:
			return 'Ping.NO_ATTEMPT(3)'
		if val == Ping.NO_RESPONSE:
			return 'Ping.NO_RESPONSE(2)'
		if val == Ping.OK:
			return 'Ping.OK(0)'
		if val == Ping.REJECT:
			return 'Ping.REJECT(1)'
	NO_ATTEMPT = 3
	NO_RESPONSE = 2
	OK = 0
	REJECT = 1
class PollingStatus(c_int):
	def __hash__(self):
		return self.value
	def __eq__(self,other):
		return self.value == other
	def __str__(self):
		val = self.value
		if val == PollingStatus.ACTIVE:
			return 'PollingStatus.ACTIVE(4)'
		if val == PollingStatus.FAILED :
			return 'PollingStatus.FAILED (0)'
		if val == PollingStatus.OK:
			return 'PollingStatus.OK(3)'
		if val == PollingStatus.READING:
			return 'PollingStatus.READING(1)'
		if val == PollingStatus.WRITING:
			return 'PollingStatus.WRITING(2)'
	ACTIVE = 4
	FAILED  = 0
	OK = 3
	READING = 1
	WRITING = 2
class TransactionStatus(c_int):
	def __hash__(self):
		return self.value
	def __eq__(self,other):
		return self.value == other
	def __str__(self):
		val = self.value
		if val == TransactionStatus.ACTIVE:
			return 'TransactionStatus.ACTIVE(1)'
		if val == TransactionStatus.IDLE:
			return 'TransactionStatus.IDLE(0)'
		if val == TransactionStatus.INERROR:
			return 'TransactionStatus.INERROR(3)'
		if val == TransactionStatus.INTRANS:
			return 'TransactionStatus.INTRANS(2)'
		if val == TransactionStatus.UNKNOWN:
			return 'TransactionStatus.UNKNOWN(4)'
	ACTIVE = 1
	IDLE = 0
	INERROR = 3
	INTRANS = 2
	UNKNOWN = 4
class Verbosity(c_int):
	def __hash__(self):
		return self.value
	def __eq__(self,other):
		return self.value == other
	def __str__(self):
		val = self.value
		if val == Verbosity.DEFAULT:
			return 'Verbosity.DEFAULT(1)'
		if val == Verbosity.TERSE:
			return 'Verbosity.TERSE(0)'
		if val == Verbosity.VERBOSE:
			return 'Verbosity.VERBOSE(2)'
	DEFAULT = 1
	TERSE = 0
	VERBOSE = 2
class PG:
	COPYRES_ATTRS = 1
	COPYRES_EVENTS = 4
	COPYRES_NOTICEHOOKS = 8
	COPYRES_TUPLES = 2
	DIAG_COLUMN_NAME = 'c'
	DIAG_CONSTRAINT_NAME = 'n'
	DIAG_CONTEXT = 'W'
	DIAG_DATATYPE_NAME = 'd'
	DIAG_INTERNAL_POSITION = 'p'
	DIAG_INTERNAL_QUERY = 'q'
	DIAG_MESSAGE_DETAIL = 'D'
	DIAG_MESSAGE_HINT = 'H'
	DIAG_MESSAGE_PRIMARY = 'M'
	DIAG_SCHEMA_NAME = 's'
	DIAG_SEVERITY = 'S'
	DIAG_SEVERITY_NONLOCALIZED = 'V'
	DIAG_SOURCE_FILE = 'F'
	DIAG_SOURCE_FUNCTION = 'R'
	DIAG_SOURCE_LINE = 'L'
	DIAG_SQLSTATE = 'C'
	DIAG_STATEMENT_POSITION = 'P'
	DIAG_TABLE_NAME = 't'
	INT64_TYPE = 'long int'
