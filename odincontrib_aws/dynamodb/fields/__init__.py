"""
Custom fields specifically for AWS Dynamo DB.

All custom fields inherit from DynamoField which introduces the
:py:meth`DynamoField.prepare_dynamo` method. This method produces a serialised
output for Dynamo DB with types included for submission to DynamoDB. In
addition the :py:meth`odin.Resource.to_python` method has been customised to
parse results in DynamoDB typed JSON.

"""
from odin import fields
from odin.serializers import datetime_iso_format, date_iso_format, time_iso_format

__all__ = ('StringField', 'IntegerField', 'FloatField', 'BooleanField',
           'DateField', 'DateTimeField', 'NaiveDateTimeField')


class DynamoField(fields.Field):
    type_descriptor = None

    def to_python(self, value):
        """
        Process a value that may include a Dynamo DB type descriptor.

        :param value: Value to process.
        :return: Python version of the specified type.

        """
        if isinstance(value, dict):
            if len(value) == 1:
                key, value = list(value.items())[0]
                if key == 'NULL':
                    return None
        return super(DynamoField, self).to_python(value)

    def prepare_dynamo(self, value):
        """
        Prepare value for dynamo and wrap with a type descriptor

        :param value:

        """
        value = self.prepare(value)
        if value is None:
            return {'NULL': True}
        else:
            return {self.type_descriptor: value}


class StringField(DynamoField, fields.StringField):
    """
    String field, utilises the `S` (string) type descriptor.
    """
    type_descriptor = 'S'


class NumericField(DynamoField):
    type_descriptor = 'N'

    def prepare_dynamo(self, value):
        if value is not None:
            value = str(value)
        return super(NumericField, self).prepare_dynamo(value)


class IntegerField(NumericField, fields.IntegerField):
    """
    Number field converted to a native Integer. Utilises the `N` (number) type
    descriptor.
    """


class FloatField(NumericField, fields.FloatField):
    """
    Number field converted to a native Float. Utilises the `N` (number) type
    descriptor.
    """


class BooleanField(DynamoField, fields.BooleanField):
    """
    Boolean field, utilises the `BOOL` type descriptor
    """
    type_descriptor = 'BOOL'


class DateField(DynamoField, fields.DateField):
    """
    Date field that represents a date in an ISO8601 date string.
    Utilises the `S` (string) type descriptor.
    """
    type_descriptor = 'S'

    def prepare(self, value):
        if value:
            value = date_iso_format(value)
        return super(DateField, self).prepare(value)


class TimeField(DynamoField, fields.TimeField):
    """
    Time field that represents a time as an ISO8601 time string.
    Utilises the `S` (string) type descriptor.
    """
    type_descriptor = 'S'

    def prepare(self, value):
        if value:
            value = time_iso_format(value)
        return super(TimeField, self).prepare(value)


class DateTimeField(DynamoField, fields.DateTimeField):
    """
    Date time field that represents a date/time in a ISO8601 date string.
    Utilises the `S` (string) type descriptor.
    """
    type_descriptor = 'S'

    def prepare(self, value):
        if value:
            value = datetime_iso_format(value)
        return super(DateTimeField, self).prepare(value)


class NaiveTimeField(DynamoField, fields.NaiveTimeField):
    """
    Time field that represents a time as an ISO8601 time string.
    Utilises the `S` (string) type descriptor.

    The naive time field differs from :py:`~DateTimeField` in the handling of
    the timezone, a timezone will not be applied if one is not specified.

    """
    type_descriptor = 'S'

    def prepare(self, value):
        value = super(NaiveTimeField, self).prepare(value)
        if value is not None:
            value = value.isoformat()
        return value


class NaiveDateTimeField(DynamoField, fields.NaiveDateTimeField):
    """
    Date time field that represents a date/time as an ISO8601 date time
    string. Utilises the `S` (string) type descriptor.

    The naive date time field differs from :py:`~DateTimeField` in the
    handling of the timezone, a timezone will not be applied if one is not
    specified.

    """
    type_descriptor = 'S'

    def prepare(self, value):
        value = super(NaiveDateTimeField, self).prepare(value)
        if value is not None:
            value = value.isoformat()
        return value
