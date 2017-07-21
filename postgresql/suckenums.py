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
	for line in lines:
		if mode == 0:

			if line == "typedef enum":
				assert next(lines) == '{'
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
				values[name] = value
				value += 1

	pid = subprocess.Popen(["cpp","-dM","-I",place],
												 stdin=subprocess.PIPE,
												 stdout=subprocess.PIPE)
	pid.stdin.write("#include <libpq-fe.h>".encode())
	pid.stdin.close()
	defines = {}
	for line in pid.stdout:
		line = line.decode('utf-8').rstrip()
		try:
			define,name,value = line.split(" ",2)
		except ValueError:
			define,name = line.split(" ",1)
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
		if not name: continue
		if '(' in name: continue
		if name and name[0]=='_': continue
		defines[name] = value
	pid.wait()
	def myrepr(v):
		if isinstance(v,str):
			if v[0] == '"' or v[0] == "'": return v
		return repr(v)
	with open("temp","wt") as out:
		for ename,values in sorted(enums.items()):
			out.write("class "+ename+":"+"\n")
			for n,v in sorted(values.items()):
				n = n.rsplit("_",1)[-1]
				out.write('\t'+n+' = '+myrepr(v)+'\n')
		for n,v in sorted(defines.items()):
			out.write(n + " = " + myrepr(v) + "\n")
	raise SystemExit

try:
	import suckenums2
except ImportError:
	generate()
	import suckenums2

import sys
sys.modules[__name__] = suckenums2
