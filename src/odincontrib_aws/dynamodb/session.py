import logging
import time
from collections import defaultdict
from typing import cast

import boto3
from botocore.exceptions import ClientError
from odin.fields import NOT_PROVIDED
from odin.resources import create_resource_from_dict
from odin.utils import getmeta, chunk

from odincontrib_aws.dynamodb.exceptions import (
    TableAlreadyExists,
    BatchLoadRetryLimitReached,
)
from odincontrib_aws.dynamodb.query import Scan, Query
from odincontrib_aws.dynamodb.table import TableOptions

logger = logging.getLogger("odincontrib_aws.dynamodb.session")

MAX_DYNAMO_BATCH_SIZE = 25


class Session:
    """
    DynamoDB Session
    """

    def __init__(self, client=None, prefix=None):
        """
        Initialise session

        :param prefix: Prefix to apply to all tables.
        :param client: Client object; if not supplied a client will be created using boto3

        """
        self.client = client or boto3.client("dynamodb")
        self.prefix = prefix

    def create_table(self, table, throughput=None, **kwargs):
        """
        Create a table in DynamoDB

        :param table: Table to create; either type or instance.
        :param throughput: Overrides for through put (read/write) capacity. This should be defined as a dictionary
            where the table override is ``None`` and indexes are named eg::

                throughput = {
                    None: {
                        'read_capacity': 10,
                        'write_capacity': 5,
                    },
                    'my_index': {
                        'read_capacity': 10,
                        'write_capacity': 5,
                    }
                }

        :param kwargs: Additional parameters (defined by Boto3 ``client.create_table``).
        :returns: Create response

        """
        throughput = throughput or {}

        meta = cast(TableOptions, getmeta(table))
        kwargs["TableName"] = meta.table_name(self)

        # Check metadata is correct
        meta.check()

        # Build key schema
        key_schema = [{"AttributeName": meta.hash_field.name, "KeyType": "HASH"}]
        if meta.range_field:
            key_schema.append(
                {"AttributeName": meta.range_field.name, "KeyType": "RANGE"}
            )
        kwargs["KeySchema"] = key_schema

        # Build attribute definitions
        attributes = {field for field in meta.key_fields}

        # Build provisioned throughput
        table_throughput = {
            "read_capacity": meta.read_capacity,
            "write_capacity": meta.write_capacity,
        }
        table_throughput.update(throughput.get(None) or {})

        kwargs["ProvisionedThroughput"] = {
            "ReadCapacityUnits": table_throughput["read_capacity"],
            "WriteCapacityUnits": table_throughput["write_capacity"],
        }

        # Add indexes and gather attributes
        if meta.local_indexes:
            indexes = []
            for idx in meta.local_indexes:
                indexes.append(idx.definition())
                attributes.update({field for field in idx.key_fields})

            kwargs["LocalSecondaryIndexes"] = indexes

        if meta.global_indexes:
            indexes = []
            for idx in meta.global_indexes:
                index_throughput = throughput.get(idx.name) or {}
                indexes.append(
                    idx.definition(
                        read_capacity=index_throughput.get(
                            "read_capacity", table_throughput["read_capacity"]
                        ),
                        write_capacity=index_throughput.get(
                            "write_capacity", table_throughput["write_capacity"]
                        ),
                    )
                )
                attributes.update({field for field in idx.key_fields})

            kwargs["GlobalSecondaryIndexes"] = indexes

        # Build attribute definitions
        kwargs["AttributeDefinitions"] = [
            {"AttributeName": field.name, "AttributeType": field.type_descriptor}
            for field in attributes
        ]

        # Call create
        try:
            return self.client.create_table(**kwargs)
        except ClientError as ex:
            if ex.response["Error"]["Code"] == u"ResourceInUseException":
                raise TableAlreadyExists(ex.response["Error"]["Message"])
            raise

    def delete_table(self, table):
        """
        Delete a table in DynamoDB

        :param table: Table to delete; either type or instance.
        :return: Delete response

        """
        meta = cast(TableOptions, getmeta(table))
        return self.client.delete_table(TableName=meta.table_name(self))

    def put_item(self, item, **kwargs):
        """
        Save complete resource to DynamoDB.

        Usage::

            >>> from odincontrib_aws import dynamodb as dynamo
            >>>
            >>> class MyTable(dynamo.Table):
            >>>     name = dynamo.StringField()
            >>>     age = dynamo.IntegerField()
            >>>
            >>> session = dynamo.Session()
            >>>
            >>> table = MyTable("Foo", 24)
            >>> session.put_item(table)

        :param item: Table instance to put into Dynamo.

        """
        meta = cast(TableOptions, getmeta(item))
        kwargs["TableName"] = meta.table_name(self)

        if hasattr(item, "on_store"):
            item.on_store(is_update=False)

        kwargs["Item"] = item.to_dynamo_dict()
        self.client.put_item(**kwargs)

    # Alias put
    save_item = put_item

    def batch_write_item(
        self,
        items,
        batch_size=MAX_DYNAMO_BATCH_SIZE,
        batch_counter_step=MAX_DYNAMO_BATCH_SIZE,
        backoff_time=5,
        retry_limit=5,
    ):
        """
        Batch write a number of items into DynamoDB

        :param items: Iterable of resources (this can be different tables).
        :param batch_size: Size of each batch.
        :param batch_counter_step: Number of batches loaded between each counter message.
        :param backoff_time: Time to delay before trying again
        :param retry_limit: Number of retry attempts before giving up.

        """
        idx = 0
        item_count = 0

        client = self.client
        for idx, batch_resources in enumerate(chunk(items, batch_size)):
            batch = defaultdict(list)
            for resource in batch_resources:
                meta = cast(TableOptions, getmeta(resource))
                batch[meta.table_name(self)].append(
                    {
                        "PutRequest": {
                            "Item": resource.to_dynamo_dict(skip_null_fields=True)
                        }
                    }
                )
                item_count += 1

            if (idx % batch_counter_step) == 0:
                logger.info("Loading batch: %s", idx)

            retry = 0
            while batch:
                result = client.batch_write_item(RequestItems=batch)
                unprocessed = result.get("UnprocessedItems")
                if unprocessed:
                    retry += 1
                    if retry > retry_limit:
                        raise BatchLoadRetryLimitReached()

                    # Assign the un-processed items to the next batch
                    batch = unprocessed

                    logger.warning(
                        "Returned %s unprocessed items, waiting %s seconds. Retry %s of %s.",
                        len(batch),
                        backoff_time,
                        retry,
                        retry_limit,
                    )
                    time.sleep(backoff_time)
                else:
                    batch = None

        logger.info("Loaded %s records in %s batches.", item_count, idx + 1)

    def update_item(self, item, fields=None, **kwargs):
        """
        Update a resource in DynamoDB

        If the on_save method is defined on the class it will be called. This
        method can return a list of additional fields (or field names) to be
        included in the update list.

        :param item: Table instance to update in Dynamo.
        :type item: odincontrib_aws.dynamodb.Table
        :param fields: Optional list of fields to update.
        :param kwargs: Additional parameters (defined by Boto3 ``client.update_item``).

        """
        meta = cast(TableOptions, getmeta(item))
        kwargs["TableName"] = meta.table_name(self)
        kwargs["Key"] = item.to_dynamo_dict(meta.key_fields)

        if fields is None:
            fields = list(meta.fields)
        elif isinstance(fields, tuple):
            fields = list(fields)
        elif not isinstance(fields, list):
            fields = [fields]

        if hasattr(item, "on_store"):
            extra_fields = item.on_store(is_update=True)
            if extra_fields:
                fields.extend(extra_fields)

        return_values = kwargs.setdefault("ReturnValues", "NONE")

        # Attributes
        if fields:
            kwargs["AttributeUpdates"] = item.to_dynamo_dict(fields, is_update=True)

        try:
            result = self.client.update_item(**kwargs)
        except ClientError as ex:
            # TODO: Error handling for common errors
            raise

        if return_values != "NONE":
            # Return a new item with the changes
            # TODO: For "New" changes update the existing item
            return create_resource_from_dict(
                result.get("Attributes"), item, copy_dict=False, full_clean=False
            )

    def get_update_item(self, table, key_value, **kwargs):
        """
        Get an item using update. This allows for update expressions to be used to apply changes to
        an entry before fetching it. Eg updating a last accessed date, or incrementing a counter.

        By defult the ``ReturnValues`` parameter is set to 'ALL_NEW'.

        :param table: Table to get/update item from.
        :type table: odincontrib_aws.dynamodb.Table
        :param key_value: Either a key value, or key pair (tuple(HASH, RANGE)).
        :param kwargs: Additional parameters (defined by Boto3 ``client.update_item``).
        :return: Instance of this resource; or None if not found

        """
        meta = cast(TableOptions, getmeta(table))
        kwargs["TableName"] = meta.table_name(self)
        kwargs["Key"] = table.format_key(key_value)
        kwargs["ReturnValues"] = "ALL_NEW"

        # TODO: Identify any field references in expressions and populate ExpressionAttributeNames

        try:
            result = self.client.update_item(**kwargs)
        except ClientError as ex:
            # ConditionalCheckFailedException is raised if an item is not found
            # and an ConditionExpression is defined.
            if ex.response["Error"]["Code"] == u"ConditionalCheckFailedException":
                return
            raise

        row = result.get("Attributes")
        if row:
            return create_resource_from_dict(
                row, table, copy_dict=False, full_clean=False
            )

    def get_item(self, table, key_value, **kwargs):
        """
        Get an item from DynamoDB

        :param table: Table to get item from.
        :type table: odincontrib_aws.dynamodb.Table
        :param key_value: Either a key value, or key pair (tuple(HASH, RANGE)).
        :param kwargs: Additional parameters (defined by Boto3 ``client.get_item``).
        :return: Instance of this resource; or None if not found

        """
        meta = cast(TableOptions, getmeta(table))
        kwargs["TableName"] = meta.table_name(self)
        kwargs["Key"] = table.format_key(key_value)

        # Get item from client
        try:
            result = self.client.get_item(**kwargs)
        except ClientError as ex:
            # TODO: Error handling for common errors
            raise

        row = result.get("Item")
        if row:
            return create_resource_from_dict(
                row, table, copy_dict=False, full_clean=False
            )

    def scan(self, table_of_index):
        """
        Perform a scan operation on a table

        :param table_of_index: Table or Index to scan; either type or instance.
        :type table_of_index: odincontrib_aws.dynamodb.Table | odincontrib_aws.dynamodb.Index
        :return: Scan instance

        """
        return Scan(self, table_of_index)

    def query(self, table_of_index, hash_value, range_value=NOT_PROVIDED):
        """
        Perform a query operation on table

        :param table_of_index: Table or Index to query; either type or instance.
        :type table_of_index: odincontrib_aws.dynamodb.Table | odincontrib_aws.dynamodb.Index
        :param hash_value: Value for the hash key.
        :param range_value:
        :return: Query instance

        """
        return Query(hash_value, range_value, self, table_of_index)
