# sigh...
import subprocess
from itertools import count
import sys
import re
decommentp = re.compile("/\\*(?:[^*]|\\*[^/])*\\*/")
def decomment(code):
	return decommentp.sub("",code)

def test_decomment():
	print(decomment("""
this is a /*nos a teh*/test of /*
decommenting *stuff /and other stuff
*/
decommenting/**/ but not decommenting this"""))
	raise SystemExit

def generate():
	pid = subprocess.Popen(["pg_config","--includedir"],stdout=subprocess.PIPE)
	place = pid.stdout.read().rstrip()
	pid.wait()
	pid = subprocess.Popen(["cpp","-I",place],stdin=subprocess.PIPE,stdout=subprocess.PIPE)
	pid.stdin.write("#include <libpq-fe.h>".encode())
	pid.stdin.close()
	code = decomment(pid.stdout.read().decode('utf-8'))
	pid.wait()

	enums = {}
	mode = 0
	values = {}
	value = 0
	ename = None
	lines = (l.strip() for l in code.split("\n"))
	lines = (l for l in lines if l)
	while True:
		try: line = next(lines)
		except StopIteration: break # python sucks
		if mode == 0:

			if line == "typedef enum":
				derp = next(lines)
				assert derp == '{'
				mode = 1
		elif mode == 1:
			if line[0] == '}':
				if ename is None:
					ename = line[2:-1] #} space name semicolon
				enums[ename] = values
				ename = None
				values = {}
				value = 0
				mode = 0
			else:
				name = line.rstrip(',')
				if '=' in name:
					name,val = name.split('=')
					value = int(val)
				name = name.split("_",1)[-1]
				values[name] = value
				value += 1

	pid = subprocess.Popen(["cpp","-dM","-I",place],
												 stdin=subprocess.PIPE,
												 stdout=subprocess.PIPE)
	pid.stdin.write("#include <libpq-fe.h>".encode())
	pid.stdin.close()
	defines = {}
	def badname(name):
		if not name: return True
		if not name.startswith('PG_'): return True
		if '(' in name: return True
		if name and name[0]=='_': return True
		return False

	for line in pid.stdout:
		line = line.decode('utf-8').rstrip()
		try:
			define,name,value = line.split(" ",2)
			if badname(name): continue
		except ValueError:
			define,name = line.split(" ",1)
			if badname(name): continue
			value = True
		else:

			if value[0] == '(':
				value = value[1:-1]
			if value.startswith("0x"):
				try: value = int(value[2:],16)
				except ValueError: pass
			else:
				try: value = int(value)
				except ValueError:
					try: value = float(value)
					except ValueError: pass
		defines[name] = value
	pid.wait()
	def myrepr(v):
		if isinstance(v,str):
			if v[0] == '"' or v[0] == "'": return v
		return repr(v)
	with open("temp","wt") as out:
		out.write("from ctypes import c_int\n")
		for ename,values in sorted(enums.items()):
			out.write("class "+ename+"(c_int):"+"\n")
			out.write("\tdef __hash__(self):\n\t\treturn self.value\n")
			out.write("\tdef __str__(self):\n")
			values = list(values.items())
			values.sort(key=lambda p: p[0])
			for n,v in values:
				out.write('\t\tif '+val+' == '+ename+'.'+n+':\n\t\t\treturn '+repr(ename+"."+n)+"\n")
			
			for n,v in values:
				out.write('\t'+n+' = '+myrepr(v)+'\n')
		for n,v in sorted(defines.items()):
			out.write(n + " = " + myrepr(v) + "\n")
	import os,sys
	name = sys.modules[__name__].__file__
	name = name[:-3]+"2.py"
	print("yay",name)
	os.rename("temp",name)

try:
	from . import suckenums2
except (ImportError,SyntaxError):
	generate()
	from . import suckenums2

import sys
sys.modules[__name__] = suckenums2
