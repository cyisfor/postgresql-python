# flatten anything (except lists and strings)
# this takes a list of tuples, generators, or iterators, and produces a generator
# that yields the items in order regardless of depth
# WITHOUT accumulating

def flatten_object(o):
	"""Given a list, possibly nested to any level, return it flattened."""
	if isinstance(o,(str,bytes,bytearray,memoryview)):
		yield o
	elif isinstance(o,list):
		# lists are special cases (don't descend)
		yield o
	else:
		try:
			it = iter(o)
			# empty lists should just conk out
			first = next(it)
			yield from flatten_object(first)
			for sub in it:
				yield from flatten_object(sub)
		except StopIteration:
			pass
		except TypeError as e:
			yield o

def flatten(*lis):
	return flatten_object(lis)

if __name__ == '__main__':
	from itertools import chain
	a = (1,2,3)
	b = tuple((a+(i,)) for i in range(3))
	c = tuple((b+("foo",)) for i in range(5))
	d = ([1,2,3],c,c,[4,5,6])
	import pprint
	pprint.pprint(d)
	for item in flatten(d):
		pprint.pprint(item)
	
