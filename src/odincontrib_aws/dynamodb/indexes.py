from odin.utils import getmeta, cached_property

__all__ = (
    "LocalIndex",
    "GlobalIndex",
    "PROJECTION_ALL",
    "PROJECTION_INCLUDE",
    "PROJECTION_KEYS_ONLY",
)


# Projection constants
PROJECTION_ALL = "ALL"
PROJECTION_KEYS_ONLY = "KEYS_ONLY"
PROJECTION_INCLUDE = "INCLUDE"


class Index:
    """
    Index definition on a table.
    """

    index_type = None

    def __init__(
        self,
        hash_key,
        range_key=None,
        projection=PROJECTION_ALL,
        includes=None,
        excludes=None,
        name=None,
    ):
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
        getmeta(cls).add_index(self)
        setattr(cls, name, self)

    @cached_property
    def hash_field(self):
        return getmeta(self.table).all_field_map[self.hash_key]

    @property
    def range_field(self):
        if self.range_key:
            return getmeta(self.table).all_field_map[self.range_key]

    @property
    def key_fields(self):
        if self.range_key:
            return self.hash_field, self.range_field
        else:
            return (self.hash_field,)  # This is a tuple

    @property
    def included_fields(self):
        """
        Used by `include` projection, returns all non-key fields that have been specified.
        """
        includes = self.includes or getmeta(self.table).all_field_map
        excludes = self.excludes or []
        return [
            field
            for field in getmeta(self.table).fields
            if field.attname in includes
            and field.attname not in excludes
            and field.attname != self.hash_key
            and field.attname != self.range_key
        ]

    def definition(self):
        """
        Generate a Index definition (used to create/update table)
        """
        # Generate KeySchema details
        key_schema = [{"AttributeName": self.hash_field.name, "KeyType": "HASH",}]
        if self.range_key:
            key_schema.append(
                {"AttributeName": self.range_field.name, "KeyType": "RANGE",}
            )

        # Generate projection details
        projection = {"ProjectionType": self.projection}
        if self.projection == PROJECTION_INCLUDE:
            projection["NonKeyAttributes"] = self.included_fields

        return {
            "IndexName": self.name,
            "KeySchema": key_schema,
            "Projection": projection,
        }


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

    index_type = "local"


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

    index_type = "global"

    def __init__(
        self,
        hash_key,
        range_key=None,
        projection=PROJECTION_ALL,
        includes=None,
        excludes=None,
        read_capacity=None,
        write_capacity=None,
        name=None,
    ):
        """
        :param read_capacity: Override table read capacity
        :param write_capacity: Override table write capacity
        """
        super(GlobalIndex, self).__init__(
            hash_key, range_key, projection, includes, excludes, name
        )
        self.read_capacity = read_capacity
        self.write_capacity = write_capacity

    def definition(self, read_capacity=None, write_capacity=None):
        """
        Generate a Index definition (used to create/update table)

        :param read_capacity: Override default read capacity
        :param write_capacity: Override default write capacity

        """
        meta = getmeta(self.table)
        definition = super(GlobalIndex, self).definition()
        definition["ProvisionedThroughput"] = {
            "ReadCapacityUnits": read_capacity
            or self.read_capacity
            or meta.read_capacity,
            "WriteCapacityUnits": write_capacity
            or self.write_capacity
            or meta.write_capacity,
        }
        return definition
