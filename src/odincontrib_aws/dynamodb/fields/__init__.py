"""
Custom fields specifically for AWS Dynamo DB.

All custom fields inherit from DynamoField which introduces the
:py:meth`DynamoField.prepare_dynamo` method. This method produces a serialised
output for Dynamo DB with types included for submission to DynamoDB. In
addition the :py:meth`odin.Resource.to_python` method has been customised to
parse results in DynamoDB typed JSON.

"""
from odin import exceptions
from odin import fields
from odin.fields import virtual
from odin.mapping import force_tuple
from odin.serializers import datetime_iso_format, date_iso_format, time_iso_format
from odincontrib_aws.dynamodb import types

__all__ = (
    "StringField",
    "IntegerField",
    "FloatField",
    "BooleanField",
    "StringSetField",
    "IntegerSetField",
    "FloatSetField",
    "DateField",
    "DateTimeField",
    "NaiveDateTimeField",
    "MultipartKeyField",
)


class DynamoField(fields.Field):
    dynamo_type = None

    def to_python(self, value):
        """
        Process a value that may include a Dynamo DB type descriptor.

        :param value: Value to process.
        :return: Python version of the specified type.

        """
        if isinstance(value, dict):
            if len(value) == 1:
                key, value = list(value.items())[0]
                if key == types.NULL:
                    return None
        return super(DynamoField, self).to_python(value)

    def prepare_dynamo(self, value):
        """
        Prepare value for dynamo and wrap with a type descriptor

        :param value:

        """
        value = self.prepare(value)
        return self.dynamo_type(value)

    @classmethod
    def format_value(cls, value, **kwargs):
        return cls.dynamo_type(value)


class StringField(DynamoField, fields.StringField):
    """
    String field, utilises the `S` (string) type descriptor.
    """

    dynamo_type = types.String


class NumericField(DynamoField):
    dynamo_type = types.Number

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

    dynamo_type = types.Boolean


SET_TYPES = {"SS", "NS", "BS"}


class DynamoSetField(fields.Field):
    type_descriptor = None

    def __iter__(self):
        """
        This does nothing however it will prevent false positive warnings from
        IDEs and static inspection tools.
        """
        pass

    def __len__(self):
        """
        This does nothing however it will prevent false positive warnings from
        IDEs and static inspection tools.
        """
        pass

    def to_python(self, value):
        """
        Process a value that may include a Dynamo DB type descriptor.

        :param value: Value to process.
        :return: Python version of the specified type.

        """
        if value is None:
            return set()

        if isinstance(value, dict):
            if len(value) == 1:
                key, value = list(value.items())[0]
                if key == "NULL":
                    return set()

        if isinstance(value, (set, list, tuple)):
            value_set = set()
            errors = {}
            for idx, item in enumerate(value):
                try:
                    value_set.add(super().to_python(item))
                except exceptions.ValidationError as ve:
                    errors[idx] = ve.error_messages

            if errors:
                raise exceptions.ValidationError(errors)

            return value_set

        else:
            msg = self.error_messages["invalid"]
            raise exceptions.ValidationError(msg)

    def prepare(self, value):
        if isinstance(value, (set, list, tuple)):
            prepare = super().prepare
            return {prepare(i) for i in value}
        return value

    def prepare_dynamo(self, value):
        """
        Prepare value for dynamo and wrap with a type descriptor

        :param value:

        """
        value = self.prepare(value)
        if value is None:
            return {"NULL": True}
        else:
            return {self.type_descriptor: list(value)}

    @classmethod
    def format_value(cls, value, **kwargs):
        return cls(**kwargs).prepare_dynamo(value)


class StringSetField(DynamoSetField, fields.StringField):
    """
    String set field
    """

    type_descriptor = "SS"


class IntegerSetField(DynamoSetField, fields.IntegerField):
    """
    Integer set field
    """

    type_descriptor = "NS"


class FloatSetField(DynamoSetField, fields.FloatField):
    """
    Float set field
    """

    type_descriptor = "NS"


####################################################################
# Extended fields


class DateField(DynamoField, fields.DateField):
    """
    Date field that represents a date in an ISO8601 date string.
    Utilises the `S` (string) type descriptor.
    """

    dynamo_type = types.String

    def prepare(self, value):
        if value:
            value = date_iso_format(value)
        return super().prepare(value)


class TimeField(DynamoField, fields.TimeField):
    """
    Time field that represents a time as an ISO8601 time string.
    Utilises the `S` (string) type descriptor.
    """

    dynamo_type = types.String

    def prepare(self, value):
        if value:
            value = time_iso_format(value)
        return super().prepare(value)


class DateTimeField(DynamoField, fields.DateTimeField):
    """
    Date time field that represents a date/time in a ISO8601 date string.
    Utilises the `S` (string) type descriptor.
    """

    dynamo_type = types.String

    def prepare(self, value):
        if value:
            value = datetime_iso_format(value)
        return super().prepare(value)


class NaiveTimeField(DynamoField, fields.NaiveTimeField):
    """
    Time field that represents a time as an ISO8601 time string.
    Utilises the `S` (string) type descriptor.

    The naive time field differs from :py:`~DateTimeField` in the handling of
    the timezone, a timezone will not be applied if one is not specified.

    """

    dynamo_type = types.String

    def prepare(self, value):
        value = super().prepare(value)
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

    dynamo_type = types.String

    def prepare(self, value):
        value = super().prepare(value)
        if value is not None:
            value = value.isoformat()
        return value


class MultipartKeyField(fields.virtual.MultiPartField):
    """
    A field whose value is the combination of several other fields.
    """

    dynamo_type = types.String
    data_type_name = "String"

    def __init__(self, field_names, separator=":", **kwargs):
        super().__init__(field_names, separator, **kwargs)

    def prepare_dynamo(self, value):
        """
        Prepare value for dynamo and wrap with a type descriptor
        """
        return self.dynamo_type(value)

    @classmethod
    def format_value(cls, values, separator=":"):
        values = force_tuple(values)
        value = separator.join(str(v) for v in values)
        return cls.dynamo_type(value)
