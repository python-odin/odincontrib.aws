"""
Custom fields specifically for AWS Dynamo DB.

All custom fields inherit from DynamoField which introduces the
:py:meth`DynamoField.prepare_dynamo` method. This method produces a serialised
output for Dynamo DB with types included for submission to DynamoDB. In
addition the :py:meth`odin.Resource.to_python` method has been customised to
parse results in DynamoDB typed JSON.

"""
from odin import fields
from odin.fields import virtual
from odin.mapping import force_tuple
from odin.serializers import datetime_iso_format, date_iso_format, time_iso_format
from odin.utils import getmeta

__all__ = ('StringField', 'IntegerField', 'FloatField', 'BooleanField',
           'DateField', 'DateTimeField', 'NaiveDateTimeField',
           'MultipartKeyField')


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

    @classmethod
    def format_value(cls, value, **kwargs):
        return cls(**kwargs).prepare_dynamo(value)


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


class MultipartKeyField(fields.Field):
    """
    A field whose value is the combination of several other fields.

    This field should be included after the field that make up the multipart value.
    """
    type_descriptor = 'S'
    data_type_name = "String"

    def __init__(self, field_names, separator=':', verbose_name=None, verbose_name_plural=None, name=None, doc_text='', key=True):
        super(MultipartKeyField, self).__init__(verbose_name, verbose_name_plural, name, doc_text=doc_text, key=key)
        self.field_names = force_tuple(field_names)
        self.separator = separator
        self._fields = None

    def __get__(self, instance, owner):
        values = [f.prepare(f.value_from_object(instance)) for f in self._fields]
        return self.separator.join(str(v) for v in values)

    def prepare_dynamo(self, value):
        """
        Prepare value for dynamo and wrap with a type descriptor
        """
        return {'S': value}

    def contribute_to_class(self, cls, name):
        super(MultipartKeyField, self).contribute_to_class(cls, name)
        setattr(cls, name, self)

    def on_resource_ready(self):
        # Extract reference to fields
        meta = getmeta(self.resource)
        try:
            self._fields = tuple(meta.field_map[name] for name in self.field_names)
        except KeyError as ex:
            raise AttributeError("Attribute {0} not found on {0!r}".format(ex, self.resource))

    @classmethod
    def format_value(cls, values, separator=':'):
        values = force_tuple(values)
        value = separator.join(str(v) for v in values)
        return {'S': value}
