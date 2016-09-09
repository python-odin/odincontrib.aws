__all__ = ('LocalIndex', 'GlobalIndex', 'PROJECTION_ALL', 'PROJECTION_INCLUDE', 'PROJECTION_KEYS_ONLY')


# Projection constants
PROJECTION_ALL = 'ALL'
PROJECTION_KEYS_ONLY = 'KEYS_ONLY'
PROJECTION_INCLUDE = 'INCLUDE'


class Index(object):
    """
    Index definition on a table.
    """
    index_type = None

    def __init__(self, hash_key, range_key=None, projection=PROJECTION_ALL, includes=None, excludes=None, name=None):
        self.name = name
        self.hash_key = hash_key
        self.range_key = range_key
        self.projection = projection
        self.includes = includes
        self.excludes = excludes

        self.table = None

    def set_attributes_from_name(self, att_name):
        if self.name is None:
            self.name = att_name

    def contribute_to_class(self, cls, name):
        self.set_attributes_from_name(name)
        self.table = cls
        cls._meta.add_index(self)
        setattr(cls, name, self)

    @property
    def hash_field(self):
        return self.table._meta.field_map[self.hash_key]

    @property
    def range_field(self):
        if self.range_key:
            return self.table._meta.field_map[self.range_key]


class LocalIndex(Index):
    """
    Include a local index

    Usage::

        >>> from odincontrib_aws import dynamodb as dynamo
        >>>
        >>> class MyTable(dynamo.Table):
        >>>     name = dynamo.StringField()
        >>>     age = dynamo.IntegerField(null=True)
        >>>
        >>>     age_keys_index = dynamo.LocalIndex('age', projection=dynamo.PROJECTION_KEYS_ONLY)
        >>>
        >>> session = dynamo.Session()
        >>> session.scan(MyTable.age_index)

    """
    index_type = 'local'


class GlobalIndex(Index):
    """
    Include a global index

    Usage::

        >>> from odincontrib_aws import dynamodb as dynamo
        >>>
        >>> class MyTable(dynamo.Table):
        >>>     name = dynamo.StringField()
        >>>     age = dynamo.IntegerField()
        >>>
        >>>     age_index = dynamo.GlobalIndex('age')
        >>>
        >>> session = dynamo.Session()
        >>> session.scan(MyTable.age_index)

    """
    index_type = 'global'
