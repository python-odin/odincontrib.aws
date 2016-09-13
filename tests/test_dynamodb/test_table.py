from odin.utils import getmeta
from .tables import Book


class MockClient(object):
    pass

book = Book(
    title="The Hitchhiker's Guide to the Galaxy",
    isbn="0-345-39180-2",
    num_pages=224,
    rrp="7.19",
    fiction=True,
    genre=None,
)


class TestTable(object):
    def test_format_table_name__with_prefix(self):
        client = MockClient()
        client.prefix = 'eek'

        actual = getmeta(Book).table_name(client)

        assert actual == "eek-library.Book"

    def test_format_table_name__without_prefix(self):
        client = MockClient()

        actual = getmeta(Book).table_name(client)

        assert actual == "library.Book"

    def test_update__single_field(self, mocker):
        """
        Handling of a single field passed to update.
        """
        client = mocker.Mock()
        client.prefix = None

        book.update(client, 'title')

        client.update_item.assert_called_once_with(
            TableName='library.Book',
            Key={'isbn': {'S': "0-345-39180-2"}},
            AttributeUpdates={
                'title': {'Action': 'PUT', 'Value': {'S': "The Hitchhiker's Guide to the Galaxy"}},
            }
        )

    def test_update__fields_in_tuple(self, mocker):
        """
        Handling of a tuple of fields passed to update.
        """
        client = mocker.Mock()
        client.prefix = None

        book.update(client, ('title', 'num_pages'))

        client.update_item.assert_called_once_with(
            TableName='library.Book',
            Key={'isbn': {'S': "0-345-39180-2"}},
            AttributeUpdates={
                'title': {'Action': 'PUT', 'Value': {'S': "The Hitchhiker's Guide to the Galaxy"}},
                'num_pages': {'Action': 'PUT', 'Value': {'N': '224'}},
            }
        )

    def test_update__fields_in_list(self, mocker):
        """
        Handling of a list of fields passed to update.
        """
        client = mocker.Mock()
        client.prefix = None

        book.update(client, ['title', 'num_pages'])

        client.update_item.assert_called_once_with(
            TableName='library.Book',
            Key={'isbn': {'S': "0-345-39180-2"}},
            AttributeUpdates={
                'title': {'Action': 'PUT', 'Value': {'S': "The Hitchhiker's Guide to the Galaxy"}},
                'num_pages': {'Action': 'PUT', 'Value': {'N': '224'}},
            }
        )
