class DynamoError(Exception):
    pass


class TableAlreadyExists(DynamoError):
    """Dynamo DB table already exists"""


class BatchLoadRetryLimitReached(DynamoError):
    """
    Retry limit hit when trying to batch load data.

    This is usually due to resource limits being exceded.
    """
