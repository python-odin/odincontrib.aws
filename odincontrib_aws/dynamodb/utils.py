import six
from odin.fields import Field
from odin.utils import getmeta


def field_smart_iter(fields, resource):
    """
    Iterator that yields field instances from a list of either field instances or field names.

    :param fields: List of either field instances or field names (can include both).
    :param resource: Resource to obtain fields from.
    :return: Field iterator

    """
    meta = getmeta(resource)
    for field in fields:
        if isinstance(field, six.string_types):
            yield meta.all_field_map[field]
        else:
            assert isinstance(field, Field)
            yield field


def domino_field_iter_items(resource, fields, required_field_names=None, skip_null_values=False):
    """
    Return an iterator that yields fields and their values from a resource.

    :param resource: Resource to iterate over.
    :param fields: Fields to use
    :param required_field_names: Fields that must be included (even if null) eg Key Fields
    :param skip_null_values: Skip values that are Null or None (as this is a special case in DynamoDB)
    :returns: an iterator that returns (field, value) tuples.

    """
    required_field_names = required_field_names or []
    for f in fields:
        value = f.value_from_object(resource)
        if skip_null_values and (f.name not in required_field_names) and (value in [None, '']):
            continue
        yield f, f.prepare_dynamo(value)
