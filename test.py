from postgresql.connection import Connection

c = Connection(dbname='derp')

c.execute("CREATE TEMPORARY TABLE derp(id integer, derp text)")
print("boop")
result = c.execute("INSERT INTO derp (id,derp) VALUES ($1,$2) RETURNING derp",(42,"answer"))
print("result:",result)

from io import StringIO
source = StringIO("23\t'fnord'\n7\t'lucky'\n13\t'unlucky'\n")
c.copy("COPY derp (id,derp) FROM STDOUT",source)

print('regular select')
result = c.execute("SELECT * from derp LIMIT 5")
print(result.fields)
print(result.types)
for row in result:
    print(row)

print('COPY TO')
for buf in c.copy("COPY derp (id,derp) TO 'derp'"):
    print(repr(buf))

