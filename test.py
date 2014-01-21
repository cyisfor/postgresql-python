from postgresql.connection import Connection

c = Connection(port=5433, dbname='derp')

c.execute("CREATE TEMPORARY TABLE derp(id integer, derp text)")

result = c.execute("INSERT INTO derp (id,derp) VALUES ($1,$2) RETURNING derp",(42,"answer"))
print("result:",result)

from io import StringIO
source = StringIO("23\t'fnord'\n7\t'lucky'\n13\t'unlucky'\n")
c.copy("COPY derp (id,derp) FROM STDOUT",source)

print('COPY OUT')
for buf in c.copy("COPY derp (id,derp) TO STDOUT"):
    print(repr(buf))

print('regular select')
result = c.execute("SELECT * from derp LIMIT 3")
print(result.fields)
print(result.types)
for row in result:
    print(row)
