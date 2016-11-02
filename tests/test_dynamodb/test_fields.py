import pytest
from odin import datetimeutil
from odincontrib_aws.dynamodb import fields

TEST_DATE = datetimeutil.datetime.date(1942, 11, 27)
TEST_TIME = datetimeutil.datetime.time(11, 12, 13, 0, datetimeutil.utc)
TEST_DATETIME = datetimeutil.datetime.datetime(1942, 11, 27, 11, 12, 13, 0, datetimeutil.utc)
TEST_NAIVE_TIME = datetimeutil.datetime.time(11, 12, 13, 0)
TEST_NAIVE_DATETIME = datetimeutil.datetime.datetime(1942, 11, 27, 11, 12, 13, 0)


@pytest.mark.parametrize(('field_type', 'value', 'expected'), (
    (fields.StringField, 'foo', {'S': 'foo'}),
    (fields.StringField, None, {'NULL': True}),

    (fields.IntegerField, 42, {'N': '42'}),
    (fields.IntegerField, None, {'NULL': True}),

    (fields.FloatField, 11.27, {'N': '11.27'}),
    (fields.FloatField, None, {'NULL': True}),

    (fields.BooleanField, True, {'BOOL': True}),
    (fields.BooleanField, False, {'BOOL': False}),
    (fields.BooleanField, None, {'NULL': True}),

    (fields.DateField, TEST_DATE, {'S': '1942-11-27'}),
    (fields.DateField, None, {'NULL': True}),

    (fields.TimeField, TEST_TIME, {'S': '11:12:13+00:00'}),
    (fields.TimeField, None, {'NULL': True}),

    (fields.NaiveTimeField, TEST_TIME, {'S': '11:12:13+00:00'}),
    (fields.NaiveTimeField, TEST_NAIVE_TIME, {'S': '11:12:13'}),
    (fields.NaiveTimeField, None, {'NULL': True}),

    (fields.DateTimeField, TEST_DATETIME, {'S': '1942-11-27T11:12:13+00:00'}),
    (fields.DateTimeField, None, {'NULL': True}),

    (fields.NaiveDateTimeField, TEST_DATETIME, {'S': '1942-11-27T11:12:13+00:00'}),
    (fields.NaiveDateTimeField, TEST_NAIVE_DATETIME, {'S': '1942-11-27T11:12:13'}),
    (fields.NaiveDateTimeField, None, {'NULL': True}),
))
def test_field__prepare_db(field_type, value, expected):
    """
    Test DynamoField.prepare method for each field type
    """
    target = field_type()
    actual = target.prepare_dynamo(value)

    if expected is None:
        assert actual is None
    else:
        assert actual == expected


@pytest.mark.parametrize(('field_type', 'value', 'expected'), (
    (fields.StringField, 'foo', 'foo'),
    (fields.StringField, {'S': 'bar'}, 'bar'),
    (fields.StringField, {'NULL': True}, None),

    (fields.IntegerField, '42', 42),
    (fields.IntegerField, {'N': '42'}, 42),
    (fields.IntegerField, {'NULL': True}, None),

    (fields.FloatField, '11.27', 11.27),
    (fields.FloatField, {'N': '11.27'}, 11.27),
    (fields.FloatField, {'NULL': True}, None),

    (fields.BooleanField, 't', True),
    (fields.BooleanField, 'f', False),
    (fields.BooleanField, True, True),
    (fields.BooleanField, False, False),
    (fields.BooleanField, {'BOOL': True}, True),
    (fields.BooleanField, {'BOOL': False}, False),
    (fields.BooleanField, {'NULL': True}, None),

    (fields.MapField(value_field=fields.StringField()), {'M': {'foo': 'bar'}}, {'foo': 'bar'}),
    (fields.MapField(value_field=fields.StringField()), {'NULL': True}, None),
    (fields.MapField(value_field=fields.StringField()), {'foo': 'bar'}, {'foo': 'bar'}),

    (fields.DateField, '1942-11-27', TEST_DATE),
    (fields.DateField, {'S': '1942-11-27'}, TEST_DATE),
    (fields.DateField, {'NULL': True}, None),

    (fields.TimeField, '11:12:13Z', TEST_TIME),
    (fields.TimeField, {'S': '11:12:13+00:00'}, TEST_TIME),
    (fields.TimeField, {'NULL': True}, None),

    (fields.DateTimeField, '1942-11-27T11:12:13Z', TEST_DATETIME),
    (fields.DateTimeField, {'S': '1942-11-27T11:12:13Z'}, TEST_DATETIME),
    (fields.DateTimeField, {'NULL': True}, None),

    (fields.NaiveTimeField, '11:12:13Z', TEST_TIME),
    (fields.NaiveTimeField, {'S': '11:12:13+00:00'}, TEST_TIME),
    (fields.NaiveTimeField, '11:12:13', TEST_NAIVE_TIME),
    (fields.NaiveTimeField, {'S': '11:12:13'}, TEST_NAIVE_TIME),
    (fields.NaiveTimeField, {'NULL': True}, None),

    (fields.NaiveDateTimeField, '1942-11-27T11:12:13Z', TEST_DATETIME),
    (fields.NaiveDateTimeField, {'S': '1942-11-27T11:12:13Z'}, TEST_DATETIME),
    (fields.NaiveDateTimeField, '1942-11-27T11:12:13', TEST_NAIVE_DATETIME),
    (fields.NaiveDateTimeField, {'S': '1942-11-27T11:12:13'}, TEST_NAIVE_DATETIME),
    (fields.NaiveDateTimeField, {'NULL': True}, None),
))
def test_field__to_python(field_type, value, expected):
    """
    Test DynamoField.to_python method for each field type
    """
    if isinstance(field_type, type):
        target = field_type()
    else:
        # Assume this is an instance
        target = field_type
    actual = target.to_python(value)

    if expected is None:
        assert actual is None
    else:
        assert actual == expected
