import pytest

from aiopynamodb.attributes import BinaryAttribute
from aiopynamodb.attributes import MapAttribute
from aiopynamodb.models import Model


def test_legacy_encoding__model() -> None:
    class _(Model):
        binary = BinaryAttribute(legacy_encoding=True)


def test_legacy_encoding__map_attribute() -> None:
    with pytest.raises(ValueError, match='legacy_encoding'):
        class _(MapAttribute):
            binary = BinaryAttribute(legacy_encoding=True)
