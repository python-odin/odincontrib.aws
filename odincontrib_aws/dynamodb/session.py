import boto3
import logging

from botocore.exceptions import ClientError
from odin.resources import create_resource_from_dict, NOT_PROVIDED
from odincontrib_aws.dynamodb.batch import MAX_DYNAMO_BATCH_SIZE, batch_write
from odincontrib_aws.dynamodb.exceptions import TableAlreadyExists

logger = logging.getLogger('odincontrib_aws.dynamodb.session')


class QueryResult(object):
    """
    Result of a Query or Scan operation.
    """
    def __init__(self, query, result):
        self.query = query
        self._result = result

    def __len__(self):
        return self.count

    def __iter__(self):
        table = self.query.table
        for item in self.raw_results:
            yield create_resource_from_dict(item, table, copy_dict=False, full_clean=False)

    @property
    def raw_results(self):
        return self._result['Items']

    @property
    def count(self):
        return self._result['Count']

    @property
    def scanned(self):
        return self._result['ScannedCount']

    @property
    def consumed_capacity(self):
        return self._result['ConsumedCapacity']

    @property
    def last_evaluated_key(self):
        return self._result.get('LastEvaluatedKey')


class PagedQueryResult(object):
    """
    Batched results of a Query or Scan operation.

    This result set will make multiple queries to Dynamo DB to get each page of results.
    """
    def __init__(self, query):
        self.query = query

        self.pages = 0
        self.count = 0
        self.scanned = 0
        self.last_page = False

    def __iter__(self):
        query = self.query
        params = query._get_params().copy()

        while True:
            logger.info("Fetching page: %s", self.pages)

            results = QueryResult(query, query._command(**params))

            # Update stats
            self.pages += 1
            self.count += results.count
            self.scanned += results.scanned
            self.last_page = results.last_evaluated_key is None

            # Yield results
            for idx, result in enumerate(results):
                yield result

            # Determine if we are done or need to get the next page
            if self.last_page:
                break
            else:
                logger.info("Returned %s of %s records; continuing from: %s",
                            results.count, self.count, results.last_evaluated_key)
                params['ExclusiveStartKey'] = results.last_evaluated_key


class QueryBase(object):
    """
    Base of Query objects
    """
    def __init__(self, session, table):
        self.session = session
        self.table = table

        self._expression_attributes = {}
        self._params = {}
        self._command = None

    def __iter__(self):
        return iter(self.all())

    def _get_params(self):
        params = self._params
        params['TableName'] = self.table.format_table_name(self.session)
        return params

    def copy(self):
        """
        Copy the Query.
        """
        query = self.__class__(self.session, self.table)
        query._params = self._params.copy()
        return query

    def single(self):
        """
        Execute operation and return a single page only.
        """
        result = self._command(**self._get_params())
        return QueryResult(self, result)

    def all(self):
        """
        Execute operation and return result object
        """
        return PagedQueryResult(self)

    def params(self, **params):
        """
        Apply params that you would execute.
        """
        self._params.update(params)

    def index(self, name):
        """
        The name of a secondary index to scan. This index can be any local
        secondary index or global secondary index.
        """
        self._params['IndexName'] = name
        return self

    def limit(self, value):
        """
        The maximum number of items to evaluate (not necessarily the number of
        matching items). If DynamoDB processes the number of items up to the
        limit while processing the results, it stops the operation and returns
        the matching values up to that point, and a key in LastEvaluatedKey to
        apply in a subsequent operation, so that you can pick up where you
        left off.
        """
        self._params['Limit'] = value
        return self

    def select(self, value='ALL_ATTRIBUTES'):
        """
        The attributes to be returned in the result. You can retrieve all item
        attributes, specific item attributes, or the count of matching items.

        - ``ALL_ATTRIBUTES`` - Returns all of the item attributes.
        - ``ALL_PROJECTED_ATTRIBUTES`` - Allowed only when querying an index.
            Retrieves all attributes that have been projected into the index.
            If the index is configured to project all attributes, this return
            value is equivalent to specifying ``ALL_ATTRIBUTES``.
        - ``COUNT`` - Returns the number of matching items, rather than the
            matching items themselves.
        - ``SPECIFIC_ATTRIBUTES`` - Returns only the attributes listed in
            AttributesToGet . This return value is equivalent to specifying
            AttributesToGet without specifying any value for Select .

        If neither Select nor AttributesToGet are specified, DynamoDB
        defaults to ``ALL_ATTRIBUTES``. You cannot use both AttributesToGet
        and Select together in a single request, unless the value for Select
        is ``SPECIFIC_ATTRIBUTES``. (This usage is equivalent to specifying
        AttributesToGet without any value for Select.)
        """
        assert value in ('ALL_ATTRIBUTES', 'ALL_PROJECTED_ATTRIBUTES', 'COUNT', 'SPECIFIC_ATTRIBUTES')

        self._params['Select'] = value
        return self

    def consumed_capacity(self, value='TOTAL'):
        """
        Determines the level of detail about provisioned throughput
        consumption that is returned in the response:

        - ``INDEXES`` - The response includes the aggregate ConsumedCapacity
            for the operation, together with ConsumedCapacity for each table
            and secondary index that was accessed. Note that some operations,
            such as GetItem and BatchGetItem , do not access any indexes at
            all. In these cases, specifying INDEXES will only return
            ConsumedCapacity information for table(s).
        - ``TOTAL`` - The response includes only the aggregate
            ConsumedCapacity for the operation.
        - ``NONE`` - No ConsumedCapacity details are included in the response.
        """
        assert value in ('INDEXES', 'TOTAL', 'NONE')

        self._params['ReturnConsumedCapacity'] = value
        return self


class Scan(QueryBase):
    """
    Perform a scan operation against a table.
    """
    def __init__(self, *args):
        super(Scan, self).__init__(*args)
        self._command = self.session.client.scan


class Query(QueryBase):
    """
    perform a query operation against a table.
    """
    def __init__(self, hash_value, *args):
        super(Query, self).__init__(*args)
        self.hash_value = hash_value
        self._range_value = NOT_PROVIDED

        self._command = self.session.client.query

    def _get_params(self):
        params = super(Query, self)._get_params()

        # Define Key conditions
        key_fields = self.table._meta.key_fields
        key_values = (self.hash_value, self._range_value)
        params['KeyConditions'] = {
            f.name: {'AttributeValueList': [f.prepare_dynamo(v)]}
            for f, v in zip(key_fields, key_values) if v is not NOT_PROVIDED
        }

        return params

    def range(self, value):
        """
        Specify the range value.
        """
        self._range_value = value
        return self


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
        kwargs['TableName'] = table.format_table_name(self)

        # Build key schema
        key_fields = table._meta.key_fields
        if 1 > len(key_fields) > 2:
            raise KeyError("A dynamo table must have either a single HASH key or a HASH/RANGE key pair.")

        key_schema = [{
            'AttributeName': key_fields[0].name,
            'KeyType': 'HASH'
        }]
        if len(key_fields) == 2:
            key_schema.append({
                'AttributeName': key_fields[1].name,
                'KeyType': 'RANGE'
            })
        kwargs['KeySchema'] = key_schema

        # Build attribute definitions
        kwargs['AttributeDefinitions'] = [{
            'AttributeName': field.name,
            'AttributeType': field.type_descriptor
        } for field in key_fields]

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
        kwargs['TableName'] = item.format_table_name(self)

        if hasattr(item, 'on_store'):
            item.on_store(is_update=False)

        kwargs['Item'] = item.to_dynamo_dict()
        self.client.put_item(**kwargs)

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
        kwargs['TableName'] = item.format_table_name(self)
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
        kwargs['TableName'] = table.format_table_name(self)
        kwargs['Key'] = table.format_key(key_value)

        # Get item from client
        result = self.client.get_item(**kwargs)
        row = result.get('Item')
        if row:
            return create_resource_from_dict(row, table, copy_dict=False, full_clean=False)

    def scan(self, table):
        """
        Perform a scan operation on a table

        :param table: Table to scan; either type or instance.
        :return: Scan instance

        """
        return Scan(self, table)

    def query(self, table, hash_value):
        """
        Perform a query operation on table

        :param table: Table to query; either type or instance.
        :param hash_value: Value for the hash key.
        :return: Query instance

        """
        return Query(hash_value, self, table)
