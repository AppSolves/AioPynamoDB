import asyncio
import os
import warnings

import pytest


@pytest.fixture(scope='module')
def ddb_url():
    """Obtain the URL of a local DynamoDB instance.

    This is meant to be used with something like DynamoDB Local:

      http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html

    It must be set up "out of band"; we merely assume it exists on
    http://localhost:8000 or a URL specified though the
    PYNAMODB_INTEGRATION_TEST_DDB_URL environment variable.
    """
    ddb_url = os.getenv("PYNAMODB_INTEGRATION_TEST_DDB_URL")
    return "http://localhost:8000" if ddb_url is None else ddb_url


@pytest.fixture(autouse=True)
def ignore_pytest_asyncio_warnings():
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="pytest_asyncio.plugin")


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
