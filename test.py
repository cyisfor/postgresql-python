from postgresql.connection import Connection

c = Connection(dbname='derp')

c.execute("DROP TABLE IF EXISTS derp")
c.execute("CREATE TABLE derp(id integer, derp text)")
print("boop")
result = c.execute("INSERT INTO derp (id,derp) VALUES ($1,$2) RETURNING derp",(42,"answer"))
print("result:",result)

print('regular select')
result = c.execute("SELECT * from derp LIMIT 5")
print(result.fields)
print(result.types)
for row in result:
    print(row)

print('COPY FROM')
from io import StringIO
source = StringIO("""23	'fnord'
7	lucky ducky
13	'unlucky'
""")
result = tuple(c.copy("COPY derp (id,derp) FROM STDIN",source))
print(result)


		
print('COPY TO')
print("to a file, tuples updated:")
print(c.execute("COPY derp (id::int,derp::text) TO '/tmp/derpderp'").tuplesUpdated)
for buf in c.copy("COPY derp (id,derp) TO STDOUT"):
    print("copy to result:",repr(buf))
print("Also returns tuples updated in the final result:")
print(c.result.tuplesUpdated)
