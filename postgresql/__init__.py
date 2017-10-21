import sys
mod = sys.modules[__name__]
class stagetwo:
	__path__ = __path__
	__spec__ = __spec__
	def __call__(self):
		print("snorgling")
		from . import init
		import sys
		sys.modules[__name__] = init
	def __getattr__(self,n):
		#print("ey?",n)
		return getattr(mod,n)
sys.modules[__name__] = stagetwo()
