from postgresql.connection import Connection

c = Connection(dbname='derp')

c.execute("DROP TABLE IF EXISTS derp")
c.execute("CREATE TABLE derp(id integer, derp text)")
print("boop")
result = c.execute("INSERT INTO derp (id,derp) VALUES ($1,$2) RETURNING derp",(42,"answer"))
print("result:",result)

from io import StringIO
source = StringIO("23\t'fnord'\n7\tlucky ducky\n13\t'unlucky'\n")
result = tuple(c.copy("COPY derp (id,derp) FROM STDIN",source))
print(result)

print('regular select')
result = c.execute("SELECT * from derp LIMIT 5")
print(result.fields)
print(result.types)
for row in result:
    print(row)

print('COPY TO')
print("to a file, tuples updated:")
print(c.execute("COPY derp (id,derp) TO '/tmp/derp'").tuplesUpdated)
for buf in c.copy("COPY derp (id,derp) TO STDOUT"):
    print(repr(buf))

print(c.result.tuplesUpdated)

