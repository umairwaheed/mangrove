import sqlalchemy


class Field(sqlalchemy.Column):
    def __get__(self, obj, obj_type):
        if obj is None:
            return self

        return getattr(obj, self._apply_suffix(self.name), self.default)

    def __set__(self, obj, value):
        self._check_type(self, value)
        setattr(obj, self._apply_suffix(self.name), value)

    def __repr__(self):
        return self._repr()

    def _repr(self, **extra_kw):
        kwargs = ['name']

        if self.key != self.name:
            kwargs.append('key')
        if self.primary_key:
            kwargs.append('primary_key')
        if not self.nullable:
            kwargs.append('nullable')
        if self.onupdate:
            kwargs.append('onupdate')
        if self.default:
            kwargs.append('default')
        if self.server_default:
            kwargs.append('server_default')

        kwargs = {k: repr(getattr(self, k)) for k in kwargs}
        kwargs.update({k: repr(v) for k, v in extra_kw.items()})

        args = ['%s=%s' % (k, v) for k, v in kwargs.items()]

        cls_name = self.__class__.__name__
        return "%s(%s)" % (cls_name, ', '.join(args))

    def _check_type(self, obj, value):
        python_type = obj.type.python_type
        if value is not None and not isinstance(value, python_type):
            name = self.name
            detail = (
                "`%s` should be of type `%s`, " % (name, python_type.__name__),
                "it was found to be of type `%s`" % type(value).__name__,
            )
            detail = ''.join(detail)
            raise ValueError(detail)

    @staticmethod
    def _apply_suffix(raw_str):
        return "_prop__%s" % raw_str


class StringField(Field):
    """

    Example
    -------

    Reprents VARCHAR in the database.

    class Person(BaseModel):
        name = StringField()

    Most databases would require a length variable as well.

    class Person(BaseModel):
        name = StringField(100)

    See sqlalchemy.String for further documentation on the arguments
    accepted.
    """
    def __init__(self, length=None, **kwargs):
        if length is None:
            _type = sqlalchemy.String
        else:
            _type = sqlalchemy.String(length)

        kwargs['type_'] = _type
        super(StringField, self).__init__(**kwargs)

    def __repr__(self):
        kwargs = {}

        length = self.type.length
        if length is not None:
            kwargs['length'] = length

        return self._repr(**kwargs)


class IntegerField(Field):
    """
    Represents int in the database.

    See sqlalchemy.Integer for further documentation
    """
    def __init__(self, **kwargs):
        kwargs['type_'] = sqlalchemy.Integer
        super(IntegerField, self).__init__(**kwargs)


class BooleanField(Field):
    """
    Represents bool in the database.

    See sqlalchemy.Boolean for further information.
    """

    def __init__(self, **kwargs):
        kwargs['type_'] = sqlalchemy.Boolean
        super(BooleanField, self).__init__(**kwargs)


class FloatField(Field):
    """
    Represents float or real field in the database

    See sqlalchemy.Float for further documentation
    """
    def __init__(self, **kwargs):
        kwargs['type_'] = sqlalchemy.Float
        super(FloatField, self).__init__(**kwargs)


class ReferenceField(sqlalchemy.ForeignKeyConstraint):
    """
    Represents ForeignKey constraint.

    >>> class Parent(Model):
            name = StringField()

    >>> class Child(Model):
            name = StringField()
            parent = ReferenceField(Parent)

    To access:
        >>> parent = Parent(name='parent')
            parent.save()

        >>> child = Child(name='child', parent=parent)
            child.save()

        `print(child.parent.name)` should output 'parent'

    See sqlalchemy.ForeignKeyConstraint for further documentation.
    """

    def __init__(self, reference, **kwargs):
        self.reference = reference
        ref_name = reference.__name__
        ref_fk_columns = reference.get_key_name()

        columns = list(self.get_fk_columns().keys())
        refcolumns = ['%s.%s' % (ref_name, c) for c in ref_fk_columns]
        super(ReferenceField, self).__init__(columns, refcolumns, **kwargs)

    def __get__(self, obj, obj_type):
        if obj is None:
            return self

        cache_name = 'cache_%s' % self.name
        if hasattr(obj, cache_name):
            return getattr(obj, cache_name)

        reference = self.reference
        query = reference.select()
        columns = self.get_fk_columns().keys()
        fk_columns = reference.get_key_name()

        for column, fk_column in zip(columns, fk_columns):
            value = getattr(obj, column)
            query.where(getattr(reference, fk_column) == value)

        _object = query.fetchone()
        setattr(obj, cache_name, _object)
        return _object

    def __set__(self, obj, value):
        columns = self.get_fk_columns().keys()
        fk_columns = self.reference.get_key_name()

        for column, fk_column in zip(columns, fk_columns):
            setattr(obj, column, getattr(value, fk_column))

    def get_fk_columns(self, reference=None):
        """
        reference:
            The reference table. If reference is None self.reference
            will be used.  
        """

        reference = reference or self.reference
        columns = reference.get_columns()
        reference_name = reference.__name__.lower()

        return {
            self.apply_prefix(reference_name, c): columns[c].__class__()
            for c in reference.get_key_name()
        }

    @staticmethod
    def apply_prefix(*args):
        return "fk_%s" % '_'.join(args)
