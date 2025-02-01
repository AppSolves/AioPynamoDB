"""
Integration tests for the model API
"""

from datetime import datetime

from aiopynamodb.models import Model
from aiopynamodb.indexes import GlobalSecondaryIndex, AllProjection, LocalSecondaryIndex
from aiopynamodb.attributes import (
    UnicodeAttribute, BinaryAttribute, UTCDateTimeAttribute, NumberSetAttribute, NumberAttribute,
    VersionAttribute)

import pytest


class LSIndex(LocalSecondaryIndex):
    """
    A model for the local secondary index
    """
    class Meta:
        projection = AllProjection()
    forum = UnicodeAttribute(hash_key=True)
    view = NumberAttribute(range_key=True)


class GSIndex(GlobalSecondaryIndex):
    """
    A model for the secondary index
    """
    class Meta:
        projection = AllProjection()
        read_capacity_units = 2
        write_capacity_units = 1
    epoch = UTCDateTimeAttribute(hash_key=True)


@pytest.mark.ddblocal
@pytest.mark.asyncio
async def test_model_integration(ddb_url):

    class TestModel(Model):
        """
        A model for testing
        """
        class Meta:
            region = 'us-east-1'
            table_name = 'pynamodb-ci'
            host = ddb_url
        forum = UnicodeAttribute(hash_key=True)
        thread = UnicodeAttribute(range_key=True)
        view = NumberAttribute(default=0)
        view_index = LSIndex()
        epoch_index = GSIndex()
        epoch = UTCDateTimeAttribute(default=datetime.now)
        content = BinaryAttribute(null=True, legacy_encoding=False)
        scores = NumberSetAttribute()
        version = VersionAttribute()

    if await TestModel.exists():
        await TestModel.delete_table()
    await TestModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)

    obj = TestModel('1', '2')
    await obj.save()
    await obj.refresh()
    obj = TestModel('foo', 'bar')
    await obj.save()
    TestModel('foo2', 'bar2')
    obj3 = TestModel('setitem', 'setrange', scores={1, 2.1})
    await obj3.save()
    await obj3.refresh()

    async with TestModel.batch_write() as batch:
        items = [TestModel('hash-{}'.format(x), '{}'.format(x)) for x in range(10)]
        for item in items:
            await batch.save(item)

    item_keys = [('hash-{}'.format(x), 'thread-{}'.format(x)) for x in range(10)]

    async for item in TestModel.batch_get(item_keys):
        print(item)

    async for item in TestModel.query('setitem', TestModel.thread.startswith('set')):
        print("Query Item {}".format(item))

    async with TestModel.batch_write() as batch:
        items = [TestModel('hash-{}'.format(x), '{}'.format(x)) for x in range(10)]
        for item in items:
            print("Batch delete")
            await batch.delete(item)

    async for item in TestModel.scan():
        print("Scanned item: {}".format(item))

    tstamp = datetime.now()
    query_obj = TestModel('query_forum', 'query_thread')
    query_obj.forum = 'foo'
    await query_obj.save()
    await query_obj.update([TestModel.view.add(1)])
    async for item in TestModel.epoch_index.query(tstamp):
        print("Item queried from index: {}".format(item))

    async for item in TestModel.view_index.query('foo', TestModel.view > 0):
        print("Item queried from index: {}".format(item.view))

    await query_obj.update([TestModel.scores.set([])])
    await query_obj.refresh()
    assert query_obj.scores is None

    print(await query_obj.update([TestModel.view.add(1)], condition=TestModel.forum.exists()))
    await TestModel.delete_table()


def test_can_inherit_version_attribute(ddb_url) -> None:

    class TestModelA(Model):
        """
        A model for testing
        """

        class Meta:
            region = 'us-east-1'
            table_name = 'pynamodb-ci-a'
            host = ddb_url

        forum = UnicodeAttribute(hash_key=True)
        thread = UnicodeAttribute(range_key=True)
        scores = NumberAttribute()
        version = VersionAttribute()

    class TestModelB(TestModelA):
        class Meta:
            region = 'us-east-1'
            table_name = 'pynamodb-ci-b'
            host = ddb_url

    with pytest.raises(ValueError) as e:
        class TestModelC(TestModelA):
            class Meta:
                region = 'us-east-1'
                table_name = 'pynamodb-ci-c'
                host = ddb_url

            version_invalid = VersionAttribute()
    assert str(e.value) == 'The model has more than one Version attribute: version, version_invalid'
