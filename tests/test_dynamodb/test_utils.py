import pytest

from odincontrib_aws.dynamodb import utils

from .tables import Book


book = Book(
    title="The Hitchhiker's Guide to the Galaxy",
    isbn="0-345-39180-2",
    num_pages=224,
    rrp="7.19",
    fiction=True,
    genre=None,
)


def test_field_smart_iter():
    """
    Ensure all fields are extracted as expected.
    """
    meta = book._meta
    actual = list(utils.field_smart_iter(['title', 'isbn', meta.field_map['num_pages'], 'fiction'], book))
    assert [
        meta.field_map['title'],
        meta.field_map['isbn'],
        meta.field_map['num_pages'],
        meta.field_map['fiction'],
    ] == actual


def test_domino_field_iter_items():
    """
    Basic extraction of fields
    """
    meta = book._meta

    actual = list(utils.domino_field_iter_items(book, meta.fields))

    assert [
        (meta.field_map['title'], {'S': "The Hitchhiker's Guide to the Galaxy"}),
        (meta.field_map['isbn'], {'S': "0-345-39180-2"}),
        (meta.field_map['num_pages'], {'N': '224'}),
        (meta.field_map['rrp'], {'N': "7.19"}),
        (meta.field_map['fiction'], {'BOOL': True}),
        (meta.field_map['genre'], {'NULL': True}),
    ] == actual


def test_domino_field_iter_items__skip_nulls():
    """
    Basic extraction of fields with null values skipped
    """
    meta = book._meta

    actual = list(utils.domino_field_iter_items(book, meta.fields, skip_null_values=True))

    assert [
        (meta.field_map['title'], {'S': "The Hitchhiker's Guide to the Galaxy"}),
        (meta.field_map['isbn'], {'S': "0-345-39180-2"}),
        (meta.field_map['num_pages'], {'N': '224'}),
        (meta.field_map['rrp'], {'N': "7.19"}),
        (meta.field_map['fiction'], {'BOOL': True}),
    ] == actual


def test_get_item__with_single_item():
    d = {"a": "b"}

    item = utils.get_item(d)

    assert item == ("a", "b")


def test_get_item__no_single_items():
    with pytest.raises(ValueError):
        utils.get_item({})


def test_get_item__multiple_items():
    with pytest.raises(ValueError):
        utils.get_item({"a": "1", "b": "2"})


def test_get_value__with_single_item():
    d = {"a": "b"}

    value = utils.get_value(d)

    assert value == "b"


def test_get_value__no_single_items():
    with pytest.raises(ValueError):
        utils.get_value({})


def test_get_value__multiple_items():
    with pytest.raises(ValueError):
        utils.get_value({"a": "1", "b": "2"})
