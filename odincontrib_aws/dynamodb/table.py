"""
Resources as as tables
"""
import logging
import odin

from odin.resources import create_resource_from_dict
from odin.utils import force_tuple, chunk
from odin.compatibility import deprecated

from odincontrib_aws.dynamodb.batch import batch_write
from odincontrib_aws.dynamodb.session import QueryResult
from odincontrib_aws.dynamodb.utils import domino_field_iter_items, field_smart_iter

__all__ = ('Table',)

logger = logging.getLogger("odincontrib.aws.dynamodb.table")


class Table(odin.Resource):
    """
    Definition of a DynamoDB Table
    """
    class Meta:
        abstract = True

    @classmethod
    def format_table_name(cls, session):
        """
        Format a tables name.
        """
        prefix = getattr(session, 'prefix', None)
        return '{}-{}'.format(prefix, cls._meta.resource_name) if prefix else cls._meta.resource_name

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

    @classmethod
    #@deprecated("To be removed in a later version please migrate to `session.get_item`.")
    def get_item(cls, client, **filters):
        """
        Get an item from DynamoDB

        :param client: DynamoDB Client
        :param filters: Any filters
        :return: Instance of this resource; or None if not found

        """
        table_name = cls.format_table_name(client)

        # Build filter keys
        key = {}
        for attr, value in filters.items():
            field = cls._meta.field_map.get(attr)
            if not field:
                raise KeyError("Unknown field: {}".format(attr))
            key[field.name] = field.prepare_dynamo(value)

        result = client.get_item(TableName=table_name, Key=key)
        row = result.get('Item')
        if row:
            return create_resource_from_dict(row, cls, copy_dict=False, full_clean=False)

    @classmethod
    #@deprecated("To be removed in a later version please migrate to `session.query`.")
    def query(cls, client, key_conditions=None, **kwargs):
        """
        Perform a query operation on table

        :param client:
        :param key_conditions:
        :return:

        """
        kwargs['TableName'] = cls.format_table_name(client)

        if key_conditions:
            for attr, ftr in key_conditions.items():
                field = cls._meta.field_map[attr]
                values = ftr['AttributeValueList']
                ftr['AttributeValueList'] = [field.prepare_dynamo(v) for v in values]
            kwargs['KeyConditions'] = key_conditions

        result = client.query(**kwargs)
        return QueryResult(cls, result)

    @classmethod
    #@deprecated("To be removed in a later version, use `session.delete_table` and `session.create_table`.")
    def empty(cls, client, batch_counter_step=25):
        """
        Empty table
        """
        table_name = cls.format_table_name(client)

        keys = client.scan(
            TableName=table_name,
            ProjectionExpression=','.join(f.name for f in cls._meta.key_fields),
        )

        # Batch delete
        idx = 0
        item_count = 0

        for idx, batch_keys in enumerate(chunk(keys['Items'], 25)):
            batch = [
                {'DeleteRequest': {'Key': key}} for key in batch_keys
            ]
            item_count += len(batch)

            if (idx % batch_counter_step) == 0:
                logger.info("Deleting batch: %s", idx)

            result = client.batch_write_item(RequestItems={
                table_name: batch
            })
            pass

        logger.info("Deleted %s records in %s batches.", item_count, idx + 1)

    @classmethod
    #@deprecated("To be removed in a later version please migrate to `session.batch_item_write`.")
    def import_csv(cls, c, client):
        """
        Import a CSV of data into a table.

        :param c: CSV file to import
        :param client: DynamoDB Client

        """
        batch_write(client, (
            create_resource_from_dict(row, cls, copy_dict=False) for row in c
        ))

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

    @deprecated("To be removed in a later version please migrate to `session.put_item`.")
    def put(self, client):
        """
        Save complete resource to DynamoDB.

        Usage::

            >>> import boto3
            >>> from odincontrib_aws import dynamodb as dynamo
            >>>
            >>> class MyTable(dynamo.Table):
            >>>     name = dynamo.StringField()
            >>>     age = dynamo.IntegerField()
            >>>
            >>> client = boto3.client("DynamoDB")
            >>>
            >>> item = MyTable("Foo", 24)
            >>> item.save(client)

        :param client: DynamoDB Client

        """
        if hasattr(self, 'on_save'):
            self.on_save(key_fields=None, is_update=False)

        table_name = self.format_table_name(client)
        client.put_item(TableName=table_name, Item=self.to_dynamo_dict())

    # Alias put with Save
    save = put

    #@deprecated("To be removed in a later version please migrate to `session.update_item`.")
    def update(self, client, fields, key_fields=None):
        """
        Update a resource in DynamoDB

        If the on_save method is defined on the class it will be called. This
        method can return a list of additional fields (or field names) to be
        included in the update list.

        :param client: DynamoDB Client
        :param fields: List of fields to update
        :param key_fields: Field(s) to mark as key; default is `key_fields` defined
            in meta. This can be multiple fields.

        """
        if isinstance(fields, tuple):
            fields = list(fields)
        elif not isinstance(fields, list):
            fields = [fields]

        key_fields = force_tuple(key_fields or self._meta.key_fields)

        if hasattr(self, 'on_save'):
            extra_fields = self.on_save(key_fields=key_fields, is_update=True)
            if extra_fields:
                fields.extend(extra_fields)

        table_name = self.format_table_name(client)
        client.update_item(
            TableName=table_name,
            Key=self.to_dynamo_dict(key_fields),
            AttributeUpdates=self.to_dynamo_dict(fields, is_update=True)
        )
