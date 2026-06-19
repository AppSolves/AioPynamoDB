API
===

High Level API
--------------

.. automodule:: aiopynamodb.models
    :members: Model

.. automodule:: aiopynamodb.attributes
    :members:

.. automodule:: aiopynamodb.indexes
    :members:

.. automodule:: aiopynamodb.transactions
    :members:

.. automodule:: aiopynamodb.pagination
    :members:

Low Level API
-------------

.. automodule:: aiopynamodb.connection
    :members: Connection, TableConnection

Exceptions
----------

.. autoexception:: aiopynamodb.exceptions.PynamoDBException
.. autoexception:: aiopynamodb.exceptions.PynamoDBConnectionError
.. autoexception:: aiopynamodb.exceptions.DeleteError
.. autoexception:: aiopynamodb.exceptions.QueryError
.. autoexception:: aiopynamodb.exceptions.ScanError
.. autoexception:: aiopynamodb.exceptions.PutError
.. autoexception:: aiopynamodb.exceptions.UpdateError
.. autoexception:: aiopynamodb.exceptions.GetError
.. autoexception:: aiopynamodb.exceptions.TableError
.. autoexception:: aiopynamodb.exceptions.TableDoesNotExist
.. autoexception:: aiopynamodb.exceptions.DoesNotExist
.. autoexception:: aiopynamodb.exceptions.TransactWriteError
.. autoexception:: aiopynamodb.exceptions.TransactGetError
.. autoexception:: aiopynamodb.exceptions.InvalidStateError
.. autoexception:: aiopynamodb.exceptions.AttributeDeserializationError
.. autoexception:: aiopynamodb.exceptions.AttributeNullError
.. autoclass:: aiopynamodb.exceptions.CancellationReason
