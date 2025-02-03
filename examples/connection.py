"""
Examples using a connection
"""
import asyncio

from aiopynamodb.connection import Connection


async def main():
    # Get a connection
    conn = Connection(host='http://localhost:8000')
    print(conn)

    # List tables
    print(await conn.list_tables())

    # Describe a table
    print(await conn.describe_table('Thread'))

    # Get an item
    print(await conn.get_item('Thread', 'hash-key', 'range-key'))

    # Put an item
    await conn.put_item('Thread', 'hash-key', 'range-key', attributes={'forum_name': 'value', 'subject': 'value'})

    # Delete an item
    await conn.delete_item('Thread', 'hash-key', 'range-key')


if __name__ == '__main__':
    asyncio.run(main())
