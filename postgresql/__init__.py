import sys
def stagetwo():
	from . import init
	import sys
	sys.modules[__name__] = init
sys.modules[__name__] = stagetwo
