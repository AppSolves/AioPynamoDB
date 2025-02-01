import pytest

import aiopynamodb.exceptions
from aiopynamodb.attributes import DiscriminatorAttribute
from aiopynamodb.attributes import DynamicMapAttribute
from aiopynamodb.attributes import ListAttribute
from aiopynamodb.attributes import MapAttribute
from aiopynamodb.attributes import NumberAttribute
from aiopynamodb.attributes import UnicodeAttribute
from aiopynamodb.indexes import AllProjection
from aiopynamodb.models import Model
from aiopynamodb.indexes import GlobalSecondaryIndex


class TestDiscriminatorIndex:

    @pytest.mark.ddblocal
    @pytest.mark.asyncio
    async def test_create_table(self, ddb_url):
        class ParentModel(Model, discriminator='Parent'):
            class Meta:
                host = ddb_url
                table_name = 'discriminator_index_test'
                read_capacity_units = 1
                write_capacity_units = 1

            hash_key = UnicodeAttribute(hash_key=True)
            cls = DiscriminatorAttribute()

        class ChildIndex(GlobalSecondaryIndex):
            class Meta:
                index_name = 'child_index'
                projection = AllProjection()
                read_capacity_units = 1
                write_capacity_units = 1

            index_key = UnicodeAttribute(hash_key=True)

        class ChildModel1(ParentModel, discriminator='Child1'):
            child_index = ChildIndex()
            index_key = UnicodeAttribute()

        # Multiple child models can share the same index
        class ChildModel2(ParentModel, discriminator='Child2'):
            child_index = ChildIndex()
            index_key = UnicodeAttribute()

        # What's important to notice is that the child_index is not defined on the parent class.
        # We're running `create_table` on the ParentModel, and expect it to know about child models
        # (through the discriminator association) and include all child models' indexes
        # during table creation.
        await ParentModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)

        model = ChildModel1()
        model.hash_key = 'hash_key1'
        model.index_key = 'bar'
        await model.save()

        model = ChildModel2()
        model.hash_key = 'hash_key2'
        model.index_key = 'baz'
        await model.save()

        model = await anext(ChildModel1.child_index.query('bar'))
        assert isinstance(model, ChildModel1)

        model = await anext(ChildModel2.child_index.query('baz'))
        assert isinstance(model, ChildModel2)

    @pytest.mark.ddblocal
    @pytest.mark.asyncio
    async def test_create_table__incompatible_indexes(self, ddb_url):
        class ParentModel(Model, discriminator='Parent'):
            class Meta:
                host = ddb_url
                table_name = 'discriminator_index_test__incompatible_indexes'
                read_capacity_units = 1
                write_capacity_units = 1

            hash_key = UnicodeAttribute(hash_key=True)
            cls = DiscriminatorAttribute()

        class ChildIndex1(GlobalSecondaryIndex):
            class Meta:
                index_name = 'child_index1'
                projection = AllProjection()
                read_capacity_units = 1
                write_capacity_units = 1

            index_key = UnicodeAttribute(hash_key=True)

        class ChildIndex2(GlobalSecondaryIndex):
            class Meta:
                index_name = 'child_index2'
                projection = AllProjection()
                read_capacity_units = 1
                write_capacity_units = 1

            # Intentionally a different type from ChildIndex1.index_key
            index_key = NumberAttribute(hash_key=True)

        # noinspection PyUnusedLocal
        class ChildModel1(ParentModel, discriminator='Child1'):
            child_index = ChildIndex1()
            index_key = UnicodeAttribute()

        # noinspection PyUnusedLocal
        class ChildModel2(ParentModel, discriminator='Child2'):
            child_index = ChildIndex2()
            index_key = UnicodeAttribute()

        # Unlike `test_create_table`, we expect this to fail because the child indexes
        # attempt to use the same attribute name for different types, thus the resulting table's
        # AttributeDefinitions would have the same attribute appear twice with conflicting types.
        with pytest.raises(aiopynamodb.exceptions.TableError, match="Cannot have two attributes with the same name"):
            await ParentModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)
