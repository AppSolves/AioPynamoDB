===========
AioPynamoDB
===========
Async fork of `PynamoDB <https://github.com/pynamodb/PynamoDB>`_ powered by `aiobotocore <https://github.com/aio-libs/aiobotocore>`_. Requires Python 3.10+.
This fork tracks `brunobelloni/AioPynamoDB <https://github.com/brunobelloni/AioPynamoDB>`_ and publishes fork builds on PyPI as ``as-aiopynamodb``.

Installation
============
From PyPI for this fork::

    $ pip install as-aiopynamodb -U

From GitHub for this fork::

    $ pip install git+https://github.com/AppSolves/AioPynamoDB#egg=as-aiopynamodb

From upstream PyPI::

    $ pip install aiopynamodb

Basic Usage
===========

Create a model that describes your DynamoDB table.

.. code-block:: python

    from aiopynamodb.models import Model
    from aiopynamodb.attributes import UnicodeAttribute

    class UserModel(Model):
        """
        A DynamoDB User
        """
        class Meta:
            table_name = "dynamodb-user"
        email = UnicodeAttribute(null=True)
        first_name = UnicodeAttribute(range_key=True)
        last_name = UnicodeAttribute(hash_key=True)

AioPynamoDB allows you to create the table if needed (it must exist before you can use it!):

.. code-block:: python

    await UserModel.create_table(read_capacity_units=1, write_capacity_units=1)

Create a new user:

.. code-block:: python

    user = UserModel("John", "Denver")
    user.email = "djohn@company.org"
    await user.save()

Now, search your table for all users with a last name of 'Denver' and whose
first name begins with 'J':

.. code-block:: python

    async for user in UserModel.query("Denver", UserModel.first_name.startswith("J")):
        print(user.first_name)

Examples of ways to query your table with filter conditions:

.. code-block:: python

    async for user in UserModel.query("Denver", UserModel.email=="djohn@company.org"):
        print(user.first_name)

Retrieve an existing user:

.. code-block:: python

    try:
        user = await UserModel.get("John", "Denver")
        print(user)
    except UserModel.DoesNotExist:
        print("User does not exist")

Advanced Usage
==============

Want to use indexes? No problem:

.. code-block:: python

    from aiopynamodb.models import Model
    from aiopynamodb.indexes import GlobalSecondaryIndex, AllProjection
    from aiopynamodb.attributes import NumberAttribute, UnicodeAttribute

    class ViewIndex(GlobalSecondaryIndex):
        class Meta:
            read_capacity_units = 2
            write_capacity_units = 1
            projection = AllProjection()
        view = NumberAttribute(default=0, hash_key=True)

    class TestModel(Model):
        class Meta:
            table_name = "TestModel"
        forum = UnicodeAttribute(hash_key=True)
        thread = UnicodeAttribute(range_key=True)
        view = NumberAttribute(default=0)
        view_index = ViewIndex()

Now query the index for all items with 0 views:

.. code-block:: python

    async for item in TestModel.view_index.query(0):
        print("Item queried from index: {0}".format(item))

It's really that simple.
