from .connection import Connection,SQLError

from contextlib import contextmanager
from itertools import count

import random

savectr = count(0)

def retransaction(connection,rollback=False):
    if not connection.inTransaction: return
    if rollback:
        connection.execute('ROLLBACK')
    else:
        connection.execute('COMMIT')
    connection.execute('BEGIN')

@contextmanager
def transaction(connection):
    if connection.inTransaction:
        # Not allowing nested transactions... because too long a transaction makes db slo-o-o-ow.
        # This totally breaks the "with" syntax as a transaction should last to the end of it.
        # But it's easier to just be in a transaction and commit periodically at known good spots.
        # One such commit in code that's considered a bad spot is not a db problem, but badly
        # written code.
        retransaction(connection)
        yield connection
        return
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
    except Exception as ee:
        try: connection.execute("ROLLBACK TO "+name)
        except SQLError as e:
            print(e)
            print('-'*60)
            print(ee)
        raise(ee)
