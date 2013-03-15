from connection import Connection

from contextlib import contextmanager

@contextmanager
def transaction(connection):
    connection.execute("BEGIN")
    try:
        yield connection
    except:
        connection.execute("ROLLBACK")
        raise
    connection.execute("COMMIT")

savectr = count(0)

@contextmanager
def saved(connection):
    name = "savepoint{}".format(savectr.__next__()))
    connection.execute("SAVE "+name)
    try:
        yield connection
    except:
        connection.execute("ROLLBACK TO "+name)
