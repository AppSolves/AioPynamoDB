import uuid
from datetime import datetime

import botocore.exceptions
import pytest
import pytest_asyncio

from aiopynamodb.attributes import (
    NumberAttribute, UnicodeAttribute, UTCDateTimeAttribute, BooleanAttribute, VersionAttribute
)
from aiopynamodb.connection import Connection
from aiopynamodb.constants import ALL_OLD
from aiopynamodb.exceptions import CancellationReason
from aiopynamodb.exceptions import DoesNotExist, TransactWriteError, InvalidStateError
from aiopynamodb.models import Model
from aiopynamodb.transactions import TransactGet, TransactWrite

IDEMPOTENT_PARAMETER_MISMATCH = 'IdempotentParameterMismatchException'
PROVISIONED_THROUGHPUT_EXCEEDED = 'ProvisionedThroughputExceededException'
RESOURCE_NOT_FOUND = 'ResourceNotFoundException'
TRANSACTION_CANCELLED = 'TransactionCanceledException'
TRANSACTION_IN_PROGRESS = 'TransactionInProgressException'
VALIDATION_EXCEPTION = 'ValidationException'


class User(Model):
    class Meta:
        region = 'us-east-1'
        table_name = 'user'

    user_id = NumberAttribute(hash_key=True)


class BankStatement(Model):
    class Meta:
        region = 'us-east-1'
        table_name = 'statement'

    user_id = NumberAttribute(hash_key=True)
    balance = NumberAttribute(default=0)
    active = BooleanAttribute(default=True)


class LineItem(Model):
    class Meta:
        region = 'us-east-1'
        table_name = 'line-item'

    user_id = NumberAttribute(hash_key=True)
    created_at = UTCDateTimeAttribute(range_key=True, default=datetime.now())
    amount = NumberAttribute()
    currency = UnicodeAttribute()


class DifferentRegion(Model):
    class Meta:
        region = 'us-east-2'
        table_name = 'different-region'

    entry_index = NumberAttribute(hash_key=True)


class Foo(Model):
    class Meta:
        region = 'us-east-1'
        table_name = 'foo'

    bar = NumberAttribute(hash_key=True)
    star = UnicodeAttribute(null=True)
    version = VersionAttribute()


TEST_MODELS = [
    BankStatement,
    DifferentRegion,
    LineItem,
    User,
    Foo
]


@pytest_asyncio.fixture(scope='module')
async def connection(ddb_url):
    conn = Connection(host=ddb_url)
    yield conn
    await conn.close()


@pytest_asyncio.fixture(scope='module', autouse=True)
async def create_tables(ddb_url):
    for m in TEST_MODELS:
        m.Meta.host = ddb_url
        await m.create_table(
            read_capacity_units=10,
            write_capacity_units=10,
            wait=True
        )

    yield

    for m in TEST_MODELS:
        if await m.exists():
            await m.delete_table()


@pytest.mark.ddblocal
@pytest.mark.asyncio
async def test_transact_write__error__idempotent_parameter_mismatch(connection):
    client_token = str(uuid.uuid4())

    async with TransactWrite(connection=connection, client_request_token=client_token) as transaction:
        transaction.save(User(1))
        transaction.save(User(2))

    with pytest.raises(TransactWriteError) as exc_info:
        # committing the first time, then adding more info and committing again
        async with TransactWrite(connection=connection, client_request_token=client_token) as transaction:
            transaction.save(User(3))
    assert exc_info.value.cause_response_code == IDEMPOTENT_PARAMETER_MISMATCH
    assert isinstance(exc_info.value.cause, botocore.exceptions.ClientError)
    assert User.Meta.table_name in exc_info.value.cause.MSG_TEMPLATE

    # ensure that the first request succeeded in creating new users
    assert await User.get(1)
    assert await User.get(2)

    with pytest.raises(DoesNotExist):
        # ensure it did not create the user from second request
        await User.get(3)


@pytest.mark.ddblocal
@pytest.mark.asyncio
async def test_transact_write__error__different_regions(connection):
    # Tip: This test *WILL* fail if run against `dynamodb-local -sharedDb` !
    with pytest.raises(TransactWriteError) as exc_info:
        async with TransactWrite(connection=connection) as transact_write:
            # creating a model in a table outside the region everyone else operates in
            transact_write.save(DifferentRegion(entry_index=0))
            transact_write.save(BankStatement(1))
            transact_write.save(User(1))
    assert exc_info.value.cause_response_code == RESOURCE_NOT_FOUND
    assert isinstance(exc_info.value.cause, botocore.exceptions.ClientError)
    assert DifferentRegion.Meta.table_name in exc_info.value.cause.MSG_TEMPLATE
    assert BankStatement.Meta.table_name in exc_info.value.cause.MSG_TEMPLATE
    assert User.Meta.table_name in exc_info.value.cause.MSG_TEMPLATE


@pytest.mark.ddblocal
@pytest.mark.asyncio
async def test_transact_write__error__transaction_cancelled__condition_check_failure(connection):
    # create a users and a bank statements for them
    await User(1).save()
    await BankStatement(1).save()

    # attempt to do this as a transaction with the condition that they don't already exist
    with pytest.raises(TransactWriteError) as exc_info:
        async with TransactWrite(connection=connection) as transaction:
            transaction.save(User(1), condition=(User.user_id.does_not_exist()))
            transaction.save(BankStatement(1), condition=(BankStatement.user_id.does_not_exist()))
    assert exc_info.value.cause_response_code == TRANSACTION_CANCELLED
    assert 'ConditionalCheckFailed' in exc_info.value.cause_response_message
    assert exc_info.value.cancellation_reasons == [
        CancellationReason(code='ConditionalCheckFailed', message='The conditional request failed'),
        CancellationReason(code='ConditionalCheckFailed', message='The conditional request failed'),
    ]
    assert isinstance(exc_info.value.cause, botocore.exceptions.ClientError)
    assert User.Meta.table_name in exc_info.value.cause.MSG_TEMPLATE
    assert BankStatement.Meta.table_name in exc_info.value.cause.MSG_TEMPLATE


@pytest.mark.ddblocal
@pytest.mark.asyncio
async def test_transact_write__error__transaction_cancelled__condition_check_failure__return_all_old(connection):
    # create a users and a bank statements for them
    await User(1).save()

    # attempt to do this as a transaction with the condition that they don't already exist
    with pytest.raises(TransactWriteError) as exc_info:
        async with TransactWrite(connection=connection) as transaction:
            transaction.save(User(1), condition=(User.user_id.does_not_exist()), return_values=ALL_OLD)
    assert exc_info.value.cause_response_code == TRANSACTION_CANCELLED
    assert 'ConditionalCheckFailed' in exc_info.value.cause_response_message
    assert exc_info.value.cancellation_reasons == [
        CancellationReason(code='ConditionalCheckFailed', message='The conditional request failed',
                           raw_item=User(1).to_dynamodb_dict()),
    ]


@pytest.mark.ddblocal
@pytest.mark.asyncio
async def test_transact_write__error__transaction_cancelled__partial_failure(connection):
    await User(2).delete()
    await BankStatement(2).save()

    # attempt to do this as a transaction with the condition that they don't already exist
    with pytest.raises(TransactWriteError) as exc_info:
        async with TransactWrite(connection=connection) as transaction:
            transaction.save(User(2), condition=(User.user_id.does_not_exist()))
            transaction.save(BankStatement(2), condition=(BankStatement.user_id.does_not_exist()))
    assert exc_info.value.cause_response_code == TRANSACTION_CANCELLED
    assert exc_info.value.cancellation_reasons == [
        None,
        CancellationReason(code='ConditionalCheckFailed', message='The conditional request failed'),
    ]


@pytest.mark.ddblocal
@pytest.mark.asyncio
async def test_transact_write__error__multiple_operations_on_same_record(connection):
    await BankStatement(1).save()

    # attempt to do a transaction with multiple operations on the same record
    with pytest.raises(TransactWriteError) as exc_info:
        async with TransactWrite(connection=connection) as transaction:
            transaction.condition_check(BankStatement, 1, condition=(BankStatement.user_id.exists()))
            transaction.update(BankStatement(1), actions=[(BankStatement.balance.add(10))])
    assert exc_info.value.cause_response_code == VALIDATION_EXCEPTION
    assert isinstance(exc_info.value.cause, botocore.exceptions.ClientError)
    assert BankStatement.Meta.table_name in exc_info.value.cause.MSG_TEMPLATE


@pytest.mark.ddblocal
@pytest.mark.asyncio
async def test_transact_get(connection):
    # making sure these entries exist, and with the expected info
    await User(1).save()
    await BankStatement(1).save()
    await User(2).save()
    await BankStatement(2, balance=100).save()

    # get users and statements we just created and assign them to variables
    async with TransactGet(connection=connection) as transaction:
        _user1_future = transaction.get(User, 1)
        _statement1_future = transaction.get(BankStatement, 1)
        _user2_future = transaction.get(User, 2)
        _statement2_future = transaction.get(BankStatement, 2)

    user1 = _user1_future.get()
    statement1 = _statement1_future.get()
    user2 = _user2_future.get()
    statement2 = _statement2_future.get()

    assert user1.user_id == statement1.user_id == 1
    assert statement1.balance == 0
    assert user2.user_id == statement2.user_id == 2
    assert statement2.balance == 100


@pytest.mark.ddblocal
@pytest.mark.asyncio
async def test_transact_get__does_not_exist(connection):
    async with TransactGet(connection=connection) as transaction:
        _user_future = transaction.get(User, 100)
    with pytest.raises(User.DoesNotExist):
        _user_future.get()


@pytest.mark.ddblocal
@pytest.mark.asyncio
async def test_transact_get__invalid_state(connection):
    async with TransactGet(connection=connection) as transaction:
        _user_future = transaction.get(User, 100)
        with pytest.raises(InvalidStateError):
            _user_future.get()


@pytest.mark.ddblocal
@pytest.mark.asyncio
async def test_transact_write(connection):
    # making sure these entries exist, and with the expected info
    await BankStatement(1, balance=0).save()
    await BankStatement(2, balance=100).save()

    # assert values are what we think they should be
    statement1 = await BankStatement.get(1)
    statement2 = await BankStatement.get(2)
    assert statement1.balance == 0
    assert statement2.balance == 100

    async with TransactWrite(connection=connection) as transaction:
        # let the users send money to one another
        # create a credit line item to user 1's account
        transaction.save(
            LineItem(user_id=1, amount=50, currency='USD'),
            condition=(LineItem.user_id.does_not_exist()),
        )
        # create a debit to user 2's account
        transaction.save(
            LineItem(user_id=2, amount=-50, currency='USD'),
            condition=(LineItem.user_id.does_not_exist()),
        )

        # add credit to user 1's account
        transaction.update(statement1, actions=[BankStatement.balance.add(50)])
        # debit from user 2's account if they have enough in the bank
        transaction.update(
            statement2,
            actions=[BankStatement.balance.add(-50)],
            condition=(BankStatement.balance >= 50)
        )

    await statement1.refresh()
    await statement2.refresh()
    assert statement1.balance == statement2.balance == 50


@pytest.mark.ddblocal
@pytest.mark.asyncio
async def test_transact_write__one_of_each(connection):
    await User(1).save()
    await User(2).save()
    statement = BankStatement(1, balance=100, active=True)
    await statement.save()

    async with TransactWrite(connection=connection) as transaction:
        transaction.condition_check(User, 1, condition=(User.user_id.exists()))
        transaction.delete(User(2))
        transaction.save(LineItem(4, amount=100, currency='USD'), condition=(LineItem.user_id.does_not_exist()))
        transaction.update(
            statement,
            actions=[
                BankStatement.active.set(False),
                BankStatement.balance.set(0),
            ]
        )

    # confirming transaction correct and successful
    assert await User.get(1)
    with pytest.raises(DoesNotExist):
        await User.get(2)

    new_line_item = await anext(LineItem.query(4, scan_index_forward=False, limit=1), None)
    assert new_line_item
    assert new_line_item.amount == 100
    assert new_line_item.currency == 'USD'

    await statement.refresh()
    assert not statement.active
    assert statement.balance == 0


@pytest.mark.ddblocal
@pytest.mark.asyncio
async def test_transaction_write_with_version_attribute(connection):
    foo1 = Foo(1)
    await foo1.save()
    foo2 = Foo(2, star='bar')
    await foo2.save()
    foo3 = Foo(3)
    await foo3.save()

    foo42 = Foo(42)
    await foo42.save()
    foo42_dup = await Foo.get(42)
    await foo42_dup.save()  # increment version w/o letting foo4 "know"

    async with TransactWrite(connection=connection) as transaction:
        transaction.condition_check(Foo, 1, condition=(Foo.bar.exists()))
        transaction.delete(foo2)
        transaction.save(Foo(4))
        transaction.update(
            foo3,
            actions=[
                Foo.star.set('birdistheword'),
            ]
        )
        transaction.update(
            foo42,
            actions=[
                Foo.star.set('last write wins'),
            ],
            add_version_condition=False,
        )

    assert (await Foo.get(1)).version == 1
    with pytest.raises(DoesNotExist):
        await Foo.get(2)
    # Local object's version attribute is updated automatically.
    assert foo3.version == 2
    assert (await Foo.get(4)).version == 1
    foo42 = await Foo.get(42)
    assert foo42.version == foo42_dup.version + 1 == 3  # ensure version is incremented
    assert foo42.star == 'last write wins'  # ensure last write wins


@pytest.mark.ddblocal
@pytest.mark.asyncio
async def test_transaction_get_with_version_attribute(connection):
    await Foo(11).save()
    await Foo(12, star='bar').save()

    async with TransactGet(connection=connection) as transaction:
        foo1_future = transaction.get(Foo, 11)
        foo2_future = transaction.get(Foo, 12)

    foo1 = foo1_future.get()
    assert foo1.version == 1
    foo2 = foo2_future.get()
    assert foo2.version == 1
    assert foo2.star == 'bar'


@pytest.mark.ddblocal
@pytest.mark.asyncio
async def test_transaction_write_with_version_attribute_condition_failure(connection):
    foo = Foo(21)
    await foo.save()

    foo2 = Foo(21)

    with pytest.raises(TransactWriteError) as exc_info:
        async with TransactWrite(connection=connection) as transaction:
            transaction.save(Foo(21))
    assert exc_info.value.cause_response_code == TRANSACTION_CANCELLED
    assert len(exc_info.value.cancellation_reasons) == 1
    assert exc_info.value.cancellation_reasons[0].code == 'ConditionalCheckFailed'
    assert isinstance(exc_info.value.cause, botocore.exceptions.ClientError)
    assert Foo.Meta.table_name in exc_info.value.cause.MSG_TEMPLATE

    with pytest.raises(TransactWriteError) as exc_info:
        async with TransactWrite(connection=connection) as transaction:
            transaction.update(
                foo2,
                actions=[
                    Foo.star.set('birdistheword'),
                ]
            )
    assert exc_info.value.cause_response_code == TRANSACTION_CANCELLED
    assert len(exc_info.value.cancellation_reasons) == 1
    assert exc_info.value.cancellation_reasons[0].code == 'ConditionalCheckFailed'
    assert Foo.Meta.table_name in exc_info.value.cause.MSG_TEMPLATE
    # Version attribute is not updated on failure.
    assert foo2.version is None

    with pytest.raises(TransactWriteError) as exc_info:
        async with TransactWrite(connection=connection) as transaction:
            transaction.delete(foo2)
    assert exc_info.value.cause_response_code == TRANSACTION_CANCELLED
    assert len(exc_info.value.cancellation_reasons) == 1
    assert exc_info.value.cancellation_reasons[0].code == 'ConditionalCheckFailed'
    assert Foo.Meta.table_name in exc_info.value.cause.MSG_TEMPLATE
