import pytest

from odincontrib_aws.dynamodb import types


@pytest.mark.parametrize("instance, expected", (
    (types.String(None), True),
    (types.String(""), True),
    (types.String("foo"), False),
    (types.Number(1), False),
))
def test_is_empty(instance, expected):
    actual = instance.is_empty()

    assert actual is expected


class TestDynamoType:
    @pytest.mark.parametrize("instance, expected", (
        (types.String(None), True),
        (types.String("foo"), False),
        (types.Number(None), True),
        (types.Number(1), False),
    ))
    def test_is_null(self, instance, expected):
        actual = instance.is_null()

        assert actual is expected
