"""
Example use of the TableConnection API
"""
import asyncio

from aiopynamodb.connection import TableConnection


async def main():
    # Get a table connection
    table = TableConnection('Thread', host='http://localhost:8000')

    # If the table doesn't already exist, the rest of this example will not work.

    # Describe the table
    print(await table.describe_table())

    # Get an item
    print(await table.get_item('hash-key', 'range-key'))

    # Put an item
    await table.put_item('hash-key', 'range-key', attributes={'forum_name': 'value'})

    # Delete an item
    await table.delete_item('hash-key', 'range-key')


if __name__ == '__main__':
    asyncio.run(main())
