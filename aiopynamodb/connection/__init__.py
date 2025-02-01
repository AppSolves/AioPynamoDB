"""
PynamoDB lowest level connection
"""

from aiopynamodb.connection.base import Connection
from aiopynamodb.connection.table import TableConnection


__all__ = [
    "Connection",
    "TableConnection",
]
