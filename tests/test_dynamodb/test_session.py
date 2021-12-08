from unittest.mock import Mock
from odin.utils import getmeta
from odincontrib_aws.dynamodb import Session

from .tables import Book


class MockClient:
    pass

book = Book(
    title="The Hitchhiker's Guide to the Galaxy",
    isbn="0-345-39180-2",
    num_pages=224,
    rrp="7.19",
    fiction=True,
    genre=None,
)


class TestSession:
    # TODO: move to table Options
    def test_format_table_name__with_prefix(self):
        client = MockClient()
        client.prefix = 'eek'

        actual = getmeta(Book).table_name(client)

        assert actual == "eek-library.Book"

    # TODO: move to table Options
    def test_format_table_name__without_prefix(self):
        client = MockClient()

        actual = getmeta(Book).table_name(client)

        assert actual == "library.Book"

    def test_update_item__single_field(self):
        """
        Handling of a single field passed to update.
        """
        session = Session(Mock())

        session.update_item(book, 'title')

        session.client.update_item.assert_called_once_with(
            TableName='library.Book',
            Key={'isbn': {'S': "0-345-39180-2"}},
            AttributeUpdates={
                'title': {'Action': 'PUT', 'Value': {'S': "The Hitchhiker's Guide to the Galaxy"}},
            },
            ReturnValues='NONE',
        )

    def test_update_item__fields_in_tuple(self):
        """
        Handling of a tuple of fields passed to update.
        """
        session = Session(Mock())

        session.update_item(book, ('title', 'num_pages'))

        session.client.update_item.assert_called_once_with(
            TableName='library.Book',
            Key={'isbn': {'S': "0-345-39180-2"}},
            AttributeUpdates={
                'title': {'Action': 'PUT', 'Value': {'S': "The Hitchhiker's Guide to the Galaxy"}},
                'num_pages': {'Action': 'PUT', 'Value': {'N': '224'}},
            },
            ReturnValues='NONE',
        )

    def test_update_item__fields_in_list(self):
        """
        Handling of a list of fields passed to update.
        """
        session = Session(Mock())

        session.update_item(book, ['title', 'num_pages'])

        session.client.update_item.assert_called_once_with(
            TableName='library.Book',
            Key={'isbn': {'S': "0-345-39180-2"}},
            AttributeUpdates={
                'title': {'Action': 'PUT', 'Value': {'S': "The Hitchhiker's Guide to the Galaxy"}},
                'num_pages': {'Action': 'PUT', 'Value': {'N': '224'}},
            },
            ReturnValues='NONE',
        )
