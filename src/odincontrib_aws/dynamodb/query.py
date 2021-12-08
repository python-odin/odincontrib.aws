import logging

from odin import bases
from odin.fields import NOT_PROVIDED
from odin.resources import create_resource_from_dict
from odin.utils import getmeta
from odin.compatibility import deprecated

from odincontrib_aws.dynamodb.indexes import Index

logger = logging.getLogger("odincontrib_aws.dynamodb.query")


class QueryResult:
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
            yield create_resource_from_dict(
                item, table, copy_dict=False, full_clean=False
            )

    @property
    def raw_results(self):
        return self._result["Items"]

    @property
    def count(self):
        return self._result["Count"]

    @property
    def scanned(self):
        return self._result["ScannedCount"]

    @property
    def consumed_capacity(self):
        return self._result["ConsumedCapacity"]

    @property
    def last_evaluated_key(self):
        return self._result.get("LastEvaluatedKey")


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
        params = query.get_params().copy()

        while True:
            logger.info("Fetching page: %s", self.pages)

            results = QueryResult(query, query.command(**params))

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
                logger.info("Returned %s of %s records.", results.count, self.count)
                break
            else:
                logger.info(
                    "Returned %s of %s records; continuing from: %s",
                    results.count,
                    self.count,
                    results.last_evaluated_key,
                )
                params["ExclusiveStartKey"] = results.last_evaluated_key


class QueryBase(bases.TypedResourceIterable):
    """
    Base of Query objects
    """

    def __init__(self, session, table_of_index):
        self.session = session

        if isinstance(table_of_index, Index):
            self.table = table_of_index.table
            self.index = table_of_index
        else:
            self.table = table_of_index
            self.index = None

        super(QueryBase, self).__init__(self.table)

        self._expression_attributes = {}
        self.params = {}
        self.command = None

    def __iter__(self):
        return iter(self.all())

    def get_params(self):
        params = self.params
        params["TableName"] = getmeta(self.table).table_name(self.session)
        if self.index:
            params["IndexName"] = self.index.name
        return params

    def copy(self):
        """
        Copy the Query.
        """
        query = self.__class__(self.session, self.table)
        query.params = self.params.copy()
        return query

    def single(self):
        """
        Execute operation and return a single page only.
        """
        result = self.command(**self.get_params())
        return QueryResult(self, result)

    def all(self):
        """
        Execute operation and return result object
        """
        return PagedQueryResult(self)

    def limit(self, value):
        """
        The maximum number of items to evaluate (not necessarily the number of
        matching items). If DynamoDB processes the number of items up to the
        limit while processing the results, it stops the operation and returns
        the matching values up to that point, and a key in LastEvaluatedKey to
        apply in a subsequent operation, so that you can pick up where you
        left off.
        """
        self.params["Limit"] = value
        return self

    def select(self, value="ALL_ATTRIBUTES"):
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
        assert value in (
            "ALL_ATTRIBUTES",
            "ALL_PROJECTED_ATTRIBUTES",
            "COUNT",
            "SPECIFIC_ATTRIBUTES",
        )

        self.params["Select"] = value
        return self

    def consumed_capacity(self, value="TOTAL"):
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
        assert value in ("INDEXES", "TOTAL", "NONE")

        self.params["ReturnConsumedCapacity"] = value
        return self

    def consistent(self, value=True):
        """
        Determines the read consistency model: If set to *true*, then the
        operation uses strongly consistent reads; otherwise, the operation
        uses eventually consistent reads.

        Strongly consistent reads are not supported on global secondary
        indexes. If you query a global secondary index with ConsistentRead set
        to *true*, you will receive a ValidationException.
        """
        self.params["ConsistentRead"] = value
        return self


class Scan(QueryBase):
    """
    Perform a scan operation against a table.
    """

    def __init__(self, *args):
        super(Scan, self).__init__(*args)
        self.command = self.session.client.scan


class Query(QueryBase):
    """
    perform a query operation against a table.
    """

    def __init__(self, hash_value, range_value, *args):
        super(Query, self).__init__(*args)
        self.hash_value = hash_value
        self.range_value = range_value

        self.command = self.session.client.query

    def get_params(self):
        params = super(Query, self).get_params()

        # Define Key conditions
        if self.index:
            key_fields = self.index.key_fields
        else:
            key_fields = getmeta(self.table).key_fields
        key_values = (self.hash_value, self.range_value)

        # TODO: Switch to KeyConditionExpression
        params["KeyConditions"] = {
            f.name: {
                "AttributeValueList": [f.prepare_dynamo(v)],
                "ComparisonOperator": "EQ",
            }
            for f, v in zip(key_fields, key_values)
            if v is not NOT_PROVIDED
        }

        return params

    @deprecated("Use the range_value provided by the session.query function")
    def range(self, value):
        """
        Specify the range value.
        """
        self.range_value = value
        return self
