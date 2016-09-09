import boto3
import logging

from botocore.exceptions import ClientError
from odin.resources import create_resource_from_dict

from odincontrib_aws.dynamodb.batch import MAX_DYNAMO_BATCH_SIZE, batch_write
from odincontrib_aws.dynamodb.exceptions import TableAlreadyExists
from odincontrib_aws.dynamodb.query import Scan, Query

logger = logging.getLogger('odincontrib_aws.dynamodb.session')


class Session(object):
    """
    DynamoDB Session
    """
    def __init__(self, client=None, prefix=None):
        """
        Initialise session

        :param prefix: Prefix to apply to all tables.
        :param client: Client object; if not supplied a client will be created using boto3

        """
        self.client = client or boto3.client('dynamodb')
        self.prefix = prefix

    def create_table(self, table, **kwargs):
        """
        Create a table in DynamoDB

        :param table: Table to create; either type or instance.
        :param kwargs: Additional parameters (defined by Boto3 ``client.create_table``).
        :returns: Create response

        """
        meta = table._meta
        kwargs['TableName'] = meta.table_name(self)

        # Check metadata is correct
        meta.check()

        key_schema = [{
            'AttributeName': meta.hash_field.name,
            'KeyType': 'HASH'
        }]
        if meta.range_field:
            key_schema.append({
                'AttributeName': meta.range_field.name,
                'KeyType': 'RANGE'
            })
        kwargs['KeySchema'] = key_schema

        # Build attribute definitions
        kwargs['AttributeDefinitions'] = [{
            'AttributeName': field.name,
            'AttributeType': field.type_descriptor
        } for field in meta.key_fields]

        if meta.local_indexes:
            kwargs['LocalSecondaryIndexes'] = [{
                'IndexName'
            } for idx in meta.local_indexes]

        # Call create
        try:
            return self.client.create_table(**kwargs)
        except ClientError as ex:
            if ex.response['Error']['Code'] == u'ResourceInUseException':
                raise TableAlreadyExists(ex.response['Error']['Message'])
            raise

    def delete_table(self, table):
        """
        Delete a table in DynamoDB

        :param table: Table to delete; either type or instance.
        :return: Delete response

        """
        return self.client.delete_table(TableName=table.format_table_name(self))

    def put_item(self, item, **kwargs):
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
            >>> session = dynamo.Session()
            >>>
            >>> item = MyTable("Foo", 24)
            >>> session.put_item(item)

        :param item: Table instance to put into Dynamo.

        """
        meta = item._meta
        kwargs['TableName'] = meta.table_name(self)

        if hasattr(item, 'on_store'):
            item.on_store(is_update=False)

        kwargs['Item'] = item.to_dynamo_dict()
        self.client.put_item(**kwargs)

    # Alias put
    save_item = put_item

    def batch_write_item(self, items, batch_size=MAX_DYNAMO_BATCH_SIZE):
        """
        Batch write a number of items into DynamoDB

        :param items: Iterable of resources (this can be different tables).
        :param batch_size: Size of batch

        """
        batch_write(self.client, items, batch_size)

    def update_item(self, item, fields=None, **kwargs):
        """
        Update a resource in DynamoDB

        If the on_save method is defined on the class it will be called. This
        method can return a list of additional fields (or field names) to be
        included in the update list.

        :param item: Table instance to update in Dynamo.
        :param fields: Optional list of fields to update.
        :param kwargs:

        """
        meta = item._meta
        kwargs['TableName'] = meta.table_name(self)
        kwargs['Key'] = item.to_dynamo_dict(meta.key_fields)

        if fields is None:
            fields = meta.fields
        elif isinstance(fields, tuple):
            fields = list(fields)
        elif not isinstance(fields, list):
            fields = [fields]

        if hasattr(item, 'on_store'):
            extra_fields = item.on_store(is_update=True)
            if extra_fields:
                fields.extend(extra_fields)

        # Attributes
        kwargs['AttributeUpdates'] = item.to_dynamo_dict(fields, is_update=True)
        return self.client.update_item(**kwargs)

    def get_item(self, table, key_value, **kwargs):
        """
        Get an item from DynamoDB

        :param table: Table to delete; either type or instance.
        :param key_value: Either a key value, or key pair (tuple(HASH, RANGE)).
        :param kwargs: Additional parameters (defined by Boto3 ``client.get_item``).
        :return: Instance of this resource; or None if not found

        """
        meta = table._meta
        kwargs['TableName'] = meta.table_name(self)
        kwargs['Key'] = table.format_key(key_value)

        # Get item from client
        result = self.client.get_item(**kwargs)
        row = result.get('Item')
        if row:
            return create_resource_from_dict(row, table, copy_dict=False, full_clean=False)

    def scan(self, table_of_index):
        """
        Perform a scan operation on a table

        :param table_of_index: Table or Index to scan; either type or instance.
        :return: Scan instance

        """
        return Scan(self, table_of_index)

    def query(self, table_of_index, hash_value):
        """
        Perform a query operation on table

        :param table_of_index: Table or Index to query; either type or instance.
        :param hash_value: Value for the hash key.
        :return: Query instance

        """
        return Query(hash_value, self, table_of_index)
