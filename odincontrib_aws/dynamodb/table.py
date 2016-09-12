"""
Resources as as tables
"""
import logging
import six

from collections import defaultdict

from odin import registration
from odin.mapping import ResourceFieldResolver
from odin.resources import ResourceOptions, ResourceType, ResourceBase
from odin.utils import force_tuple, cached_property

from odincontrib_aws.dynamodb.utils import domino_field_iter_items, field_smart_iter

__all__ = ('Table',)

logger = logging.getLogger("odincontrib.aws.dynamodb.table")


class TableOptions(ResourceOptions):
    """
    Table specifc options
    """
    META_OPTION_NAMES = ResourceOptions.META_OPTION_NAMES + (
        'read_capacity', 'write_capacity'
    )

    def __init__(self, meta):
        super(TableOptions, self).__init__(meta)

        self.indexes = defaultdict(list)
        self.read_capacity = 1
        self.write_capacity = 1

    def add_index(self, index):
        """
        Register an index
        """
        self.indexes[index.index_type].append(index)

    def check(self):
        if not (0 < len(self.key_fields) < 3):
            raise KeyError("A dynamo table must have either a single HASH key or a HASH/RANGE key pair.")

    def table_name(self, session=None):
        """
        Generate a table name
        """
        if session:
            prefix = getattr(session, 'prefix', None)
            if prefix:
                return '{}-{}'.format(prefix, self.resource_name)
        return self.resource_name

    @cached_property
    def hash_field(self):
        key_fields = self.key_fields
        if key_fields:
            return self.key_fields[0]

    @cached_property
    def range_field(self):
        key_fields = self.key_fields
        if len(key_fields) > 1:
            return self.key_fields[1]

    @property
    def global_indexes(self):
        return self.indexes['global']

    @property
    def local_indexes(self):
        return self.indexes['local']


class TableType(ResourceType):
    meta_options = TableOptions


@six.add_metaclass(TableType)
class Table(ResourceBase):
    """
    Definition of a DynamoDB Table
    """
    class Meta:
        abstract = True

    @classmethod
    def format_key(cls, key_values):
        """
        Format a key (or key pair) from values supplied.

        :param key_values:

        """
        key_values = force_tuple(key_values)
        key_fields = cls._meta.key_fields
        if len(key_values) != len(key_fields):
            raise KeyError("This table uses a multi part key, `key_value` must be pair of values in a tuple.")
        return {f.name: f.prepare_dynamo(v) for f, v in zip(key_values, key_fields)}

    def to_dynamo_dict(self, fields=None, is_update=False, skip_null_fields=False):
        """
        Convert this resource into a "dynamo" `dict` of field_name, type/value
        pairs.

        .. note::
            This method is not recursive, it only operates on this single
            resource, any sub resources are returned as is. The use case that
            prompted the creation of this method is within codecs when a
            resource must be converted into a type that can be serialised,
            these codecs then operate recursively on the returned `dict`.

        """
        if fields:
            # Ensure fields have been resolved
            fields = (f for f in field_smart_iter(fields, self))
        else:
            fields = self._meta.fields
        if is_update:
            # Return with the Value/Action block
            return {f.name: {"Value": v, "Action": "PUT"}
                    for f, v in domino_field_iter_items(self, fields, skip_null_fields)}
        else:
            return {f.name: v for f, v in domino_field_iter_items(self, fields, skip_null_fields)}


# Register tables as mappable by a standard resource field resolver
registration.register_field_resolver(ResourceFieldResolver, Table)
