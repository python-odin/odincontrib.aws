class DynamoError(Exception):
    pass


class TableAlreadyExists(DynamoError):
    """Dynamo DB table already exists"""
