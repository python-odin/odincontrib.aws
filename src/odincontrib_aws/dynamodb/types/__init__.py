from boto3.dynamodb import types
from boto3.dynamodb.types import NULL

__all__ = (
    "String",
    "StringSet",
    "Number",
    "NumberSet",
    "Integer",
    "IntegerSet",
    "Float",
    "FloatSet",
    "Decimal",
    "DecimalSet",
    "Boolean",
    "Binary",
    "BinarySet",
    "List",
    "Map",
)


class DynamoType(dict):
    type_descriptor = None

    def __init__(self, value):
        if value is None:
            kwargs = {NULL: True}
        else:
            kwargs = {self.type_descriptor: value}
        super(DynamoType, self).__init__(**kwargs)

    def is_empty(self):
        return False

    def is_null(self):
        return self.values()[0] is None


class String(DynamoType):
    type_descriptor = types.STRING

    def is_empty(self):
        value = self.values()[0]
        return value is not None and len(value) == 0


class StringSet(DynamoType):
    type_descriptor = types.STRING_SET


class Number(DynamoType):
    type_descriptor = types.NUMBER


# Aliases of Number
Integer = Number
Float = Number
Decimal = Number


class NumberSet(DynamoType):
    type_descriptor = types.NUMBER_SET


# Aliases of NumberSet
IntegerSet = NumberSet
FloatSet = NumberSet
DecimalSet = NumberSet


class Boolean(DynamoType):
    type_descriptor = types.BOOLEAN


class Binary(DynamoType):
    type_descriptor = types.BINARY


class BinarySet(DynamoType):
    type_descriptor = types.BINARY_SET


class List(DynamoType):
    type_descriptor = types.LIST


class Map(DynamoType):
    type_descriptor = types.MAP
