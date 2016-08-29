from odincontrib_aws import dynamodb as odin_dynamo


class Book(odin_dynamo.Table):
    class Meta:
        namespace = 'library'
        key_field_name = 'isbn'

    title = odin_dynamo.StringField()
    isbn = odin_dynamo.StringField()
    num_pages = odin_dynamo.IntegerField()
    rrp = odin_dynamo.FloatField(default=20.4, use_default_if_not_provided=True)
    fiction = odin_dynamo.BooleanField(is_attribute=True)
    genre = odin_dynamo.StringField(choices=(
        ('sci-fi', 'Science Fiction'),
        ('fantasy', 'Fantasy'),
        ('biography', 'Biography'),
        ('others', 'Others'),
        ('computers-and-tech', 'Computers & technology'),
    ))
    # published = odin.TypedArrayField(odin.DateTimeField())
    # authors = odin.ArrayOf(Author, use_container=True)
    # publisher = odin.DictAs(Publisher, null=True)

    def __eq__(self, other):
        if other:
            return vars(self) == vars(other)
        return False
