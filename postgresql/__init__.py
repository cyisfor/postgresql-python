import sys
mod = sys.modules[__name__]
class stagetwo:
	__path__ = __path__
	__spec__ = __spec__
	_init = None
	def __call__(self):
		print("snorgling",__name__)
		import traceback
		traceback.print_stack()
		if self._init is None:
			# ugh, python, whyyy
			from . import init
			print("yay",init)
			self._init = init
		import sys
		sys.modules[__name__] = self._init
		return self._init
	def __getattr__(self,n):
		#print("ey?",n)
		if n in {'Connection','transaction','retransaction','saved','SQLError'}:
			# triggered!
			initt = self()
			return getattr(initt,n)
		return getattr(mod,n)
sys.modules[__name__] = stagetwo()
