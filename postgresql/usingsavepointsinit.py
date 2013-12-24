from .connection import Connection,SQLError

from contextlib import contextmanager
from itertools import count

import random

savectr = count(0)

def retransaction(connection,rollback=True):
    if connection.savePoint:
        if rollback:
            connection.execute("ROLLBACK TO SAVEPOINT "+connection.savePoint)
        else:
            connection.execute('RELEASE SAVEPOINT '+name)
        connection.execute('SAVEPOINT '+name)
    else:
        if rollback:
            connection.execute('ROLLBACK')
        else:
            connection.execute('COMMIT')
        connection.execute('BEGIN')

@contextmanager
def transaction(connection):
    if connection.inTransaction:
        name = "transaction{}".format(next(savectr))
        connection.execute("SAVEPOINT "+name)
        oldpoint = connection.savePoint
        connection.savePoint = name
        try:
            yield connection
        except:
            connection.execute("ROLLBACK TO SAVEPOINT "+name)
            raise
        finally:
            connection.savePoint = oldpoint
        connection.execute('RELEASE SAVEPOINT '+name)
        return # no commit until the end!
    connection.inTransaction = True
    connection.execute("BEGIN")
    try:
        yield connection
    except:
        if connection.verbose:
            import traceback
            traceback.print_exc()
        connection.execute("ROLLBACK")
        raise
    finally:
        connection.inTransaction = False
    connection.execute("COMMIT")

@contextmanager
def saved(connection):
    name = "savepoint{}".format(next(savectr))
    connection.execute("SAVEPOINT "+name)
    try:
        yield connection
        connection.execute("RELEASE SAVEPOINT "+name)
    except:
        connection.execute("ROLLBACK TO "+name)
        raise
