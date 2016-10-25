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
    Table specific options
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
    def all_field_map(self):
        return dict((f.attname, f) for f in self.all_fields)

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

    >>> class Book(Table):
    >>>     class Meta:
    >>>         # Meta information

    """
    class Meta:
        abstract = True

    def __init__(self, *args, **kwargs):
        super(Table, self).__init__(*args, **kwargs)
        self._bound_session = None

    @property
    def is_bound_to_session(self):
        return self._bound_session is not None

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
        return {f.name: f.prepare_dynamo(v) for v, f in zip(key_values, key_fields)}

    def bind_to_session(self, session):
        """
        Bind this table to a particular session

        :param session: Session to bind to, this can also be `None` to unbind session.
        :type session: odincontrib_aws.dynamodb.Session | None

        """
        self._bound_session = session

    def to_dynamo_dict(self, fields=None, is_update=False, skip_null_fields=True):
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
        required_field_names = None
        if fields:
            # Ensure fields have been resolved
            fields = (f for f in field_smart_iter(fields, self))
        else:
            fields = self._meta.all_fields
            required_field_names = [f.name for f in self._meta.key_fields]
        if is_update:
            # Return with the Value/Action block
            return {f.name: {"Value": v, "Action": "PUT"}
                    for f, v in domino_field_iter_items(self, fields, None, skip_null_fields)}
        else:
            return {
                f.name: v for f, v in domino_field_iter_items(self, fields, required_field_names, skip_null_fields)
            }

# Register tables as mappable by a standard resource field resolver
registration.register_field_resolver(ResourceFieldResolver, Table)
