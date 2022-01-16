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
        super().__init__(**kwargs)

    def is_empty(self):
        return False

    def is_null(self):
        return NULL in self

    @property
    def value(self):
        return None if NULL in self else self.get(self.type_descriptor)


class String(DynamoType):
    type_descriptor = types.STRING

    def is_empty(self):
        return not bool(self.value)


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
