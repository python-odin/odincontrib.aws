from odincontrib_aws import dynamodb as dynamo


class Book(dynamo.Table):
    class Meta:
        namespace = 'library'

    title = dynamo.StringField()
    isbn = dynamo.StringField(key=True)
    num_pages = dynamo.IntegerField()
    rrp = dynamo.FloatField(default=20.4, use_default_if_not_provided=True)
    fiction = dynamo.BooleanField(is_attribute=True)
    genre = dynamo.StringField(choices=(
        ('sci-fi', 'Science Fiction'),
        ('fantasy', 'Fantasy'),
        ('biography', 'Biography'),
        ('others', 'Others'),
        ('computers-and-tech', 'Computers & technology'),
    ))
    # published = odin.TypedArrayField(odin.DateTimeField())
    # authors = odin.ArrayOf(Author, use_container=True)
    # publisher = odin.DictAs(Publisher, null=True)

    genre_index = dynamo.GlobalIndex('genre', 'isbn')

    def __eq__(self, other):
        if other:
            return vars(self) == vars(other)
        return False
