import six
from odin.fields import Field


def field_smart_iter(fields, resource):
    """
    Iterator that yields field instances from a list of either field instances or field names.

    :param fields: List of either field instances or field names (can include both).
    :param resource: Resource to obtain fields from.
    :return: Field iterator

    """
    meta = resource._meta
    for field in fields:
        if isinstance(field, six.string_types):
            yield meta.field_map[field]
        else:
            assert isinstance(field, Field)
            yield field


def domino_field_iter_items(resource, fields, skip_null_values=False):
    """
    Return an iterator that yields fields and their values from a resource.

    :param resource: Resource to iterate over.
    :param fields: Fields to use
    :param skip_null_values: Skip values that are Null or None (as this is a special case in DynamoDB)
    :returns: an iterator that returns (field, value) tuples.

    """
    for f in fields:
        value = f.value_from_object(resource)
        if skip_null_values and value is None:
            continue
        yield f, f.prepare_dynamo(value)
