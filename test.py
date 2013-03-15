import connection

c = connection.Connection(port=5433, dbname='derp')

for buf in c.copy("COPY images (width,height) TO STDOUT"):
    print(buf)

result = c.execute("SELECT * from media LIMIT 5")
print(result.fields)
for row in result:
    print(row)
