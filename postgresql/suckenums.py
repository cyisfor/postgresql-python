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
	enums = {}

	pid = subprocess.Popen(["pg_config","--includedir"],stdout=subprocess.PIPE)
	place = pid.stdout.read().rstrip()
	pid.wait()
	pid = subprocess.Popen(["cpp","-I",place],stdin=subprocess.PIPE,stdout=subprocess.PIPE)
	pid.stdin.write("#include <libpq-fe.h>".encode())
	pid.stdin.close()
	code = decomment(pid.stdin.read().decode('utf-8'))
	
	mode = 0
	for line in code.split("\n"):
		line = line..strip()
		line = line.split('/*',1)[0]
		if not line: continue
		if mode == 0:
				if 'enum' in line:
						name = line.split('enum',1)[1].strip()
						if not name:
								mode = 1
						else:
								mode = 2
						counter = count(0)
		elif mode == 1:
				if '{' in line:
						name = line.split('{',1)[0]
						mode = 3
				else:
						name = line
						mode = 2
		elif mode == 2:
				if '{' in line:
						mode = 3
				else:
						mode = 0
		elif mode == 3:
				if '}' in line:
						if not name:
								name = line.split('}',1)[1].strip(';').strip()
								enums[name] = enums.get('')
								del enums['']
								for n,i in enums[name]:
										setattr(sys.modules[__name__],n,i)
						mode = 0
						continue;
				value = line.strip().rstrip(',')
				if not value: continue
				if '=' in value:
						value,num = value.split('=')
						counter = count(int(num))
						value = value.rstrip()
				i = next(counter)
				v = enums.get(name)
				if v:
						v.append((value,i))
				else:
						v = [(value,i)]
						enums[name] = v
				if name:
						setattr(sys.modules[__name__],value,i)
pid.wait()
pid = subprocess.Popen(["cpp","-dM","-I",place],stdin=subprocess.PIPE,
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
				try: value = int(value)
				except ValueError:
						try: value = float(value)
						except ValueError: pass
		if not name: continue
		if '(' in name: continue
		if name and name[0]=='_': continue
		defines[name] = value
		setattr(sys.modules[__name__],name,value)
pid.wait()



try:
	import suckenums2
except ImportError:
	generate()
	import suckenums2

import sys
sys.modules[__name__] = suckenums2
