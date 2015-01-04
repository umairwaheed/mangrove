import json
import sqlalchemy
import collections

import query
import fields
import metadata
import exceptions


class Meta(type):
    """Initialize the class attributes

    The meta class is used to assign names to the fields of the model
    """
    def __new__(metacls, name, bases, namespace, **kwargs):
        cls = type.__new__(metacls, name, bases, namespace, **kwargs)

        # Add foreign key columns to the model
        for name, constraint in cls.get_constraints().items():
            # we need to assign name every time class is created.
            constraint.name = constraint.name or constraint.apply_prefix(name)
            for name, column in constraint.get_fk_columns().items():
                setattr(cls, name, column)

        for name, column in cls.get_columns().items():
            # `column.name` is the name assigned by the user in the
            # constructor Field(name=[]...), it is given preference.
            column.name = column.name or name

        if cls.__name__ != "Model" and not cls.abstract:
            metacls._init_table(cls)

        return cls

    @staticmethod
    def _init_table(cls):
        """Initialize the underlying SQLAlchemy table

        Creates all the columns of the table, adds a primary key
        column if necessary and creates the table in the database.
        """

        metadata = cls.metadata
        columns = cls.get_columns()

        if not cls.get_key_name():
            # Primary key not found! Add primary key column.
            key_name = 'id'
            if key_name in columns:
                detail = (
                    "`id` already defined and therefore cannot be "
                    "used for automatic primary key creation. Please "
                    "either define a primary key or rename `id`."
                )
                raise exceptions.InvalidKeyFieldError(detail)

            IntegerField = fields.IntegerField
            key_col = IntegerField(name=key_name, primary_key=True)
            setattr(cls, key_name, key_col)

            columns[key_name] = key_col

        # add columns and constraints to tbe table
        table_name = cls.__name__
        if table_name not in metadata.tables:
            table = sqlalchemy.Table(cls.__name__, metadata)
            try:
                for name, column in columns.items():
                    column = column.copy()
                    table.append_column(column)

                for name, constraint in cls.get_constraints().items():
                    if hasattr(constraint, 'parent'):
                        constraint = constraint.copy()

                    table.append_constraint(constraint)

                metadata.create_all(tables=[table])
            except Exception:
                # in case of exception remove the table from the metadata
                metadata.remove(table)
                raise # re-raise the exception


class Model():
    """Base class for all models

    Attributes
    -----------

    abstract:
        If `True` DB table will not be created


    Model Example
    -------------

    class Person(Model):
        name = fields.StringField()
        age = fields.IntegerField()


    Primary Key
    -----------

    To add primary key to the table add `primary_key=True` to columns
    """

    __metaclass__ = Meta
    metadata = metadata.MetaData.get_metadata()
    abstract = False

    def __init__(self, **kwargs):
        for name, value in kwargs.items():
            setattr(self, name, value)

    def __repr__(self):
        args = ', '.join('%s=%s' % (p, repr(getattr(self, p)))
                         for p in self.get_columns())
        cls_name = self.__class__.__name__
        return "%s(%s)" % (cls_name, args)

    def __iter__(self):
        return ((p, getattr(self, p)) for p in self.get_columns())

    @classmethod
    def get_key_name(cls):
        """Name of the column(s) that form primary key

        Key name is a tuple of alphabetically sorted names of
        columns that have primary_key == True.
        """

        columns = cls.get_columns().values()
        return tuple(sorted(p.name for p in columns if p.primary_key))

    @classmethod
    def get_table(cls):
        """Return the underlying SQLAlchemy table

        In case the table is not found it is created, we need to do
        this because this function can be called from `select` as well
        and queries can be performed on the table without making the
        instance of the model.
        """

        table_name = cls.__name__
        tables = cls.metadata.tables
        return tables[table_name]

    @classmethod
    def _get_items_from_dict(cls, item_type):
        #ChainMap = collections.ChainMap
        #items = ChainMap(*[c.__dict__ for c in cls.mro()]).items()
        items = [i for base in cls.mro() for i in base.__dict__.items()]
        return {k: v for k, v in items if isinstance(v, item_type)}

    @classmethod
    def get_columns(cls):
        return cls._get_items_from_dict(sqlalchemy.Column)

    @classmethod
    def get_constraints(cls):
        return cls._get_items_from_dict(sqlalchemy.Constraint)

    @classmethod
    def select(cls, columns=[]):
        return query.Query(cls, columns=columns)

    @classmethod
    def get_by_key(cls, key_map):
        query = cls.select()
        for column, value in key_map.items():
            query.where(column==value)

        return query.fetchone()

    @property
    def key(self):
        _key = tuple(getattr(self, p) for p in self.get_key_name())
        return tuple(filter(None, _key))

    def save(self):
        """ Will only execute insert statement

        It is an error to use this method for record which
        already exists.
        """

        data = {p: getattr(self, p) for p in self.get_columns()}
        stmt = self.get_table().insert().values(**data)
        result = self.metadata.bind.connect().execute(stmt)

        # set the key on the model
        key_name = self.get_key_name()
        for col_name, col_value in zip(key_name, result.inserted_primary_key):
            setattr(self, col_name, col_value)

        return result

    def update(self, exclude=[]):
        """ Updates an entity

        Issues update statement to the database. This function will
        update the corressponding row with the properties of the object.

        Arguments
        ---------
        @exclude: A list of column names that you want to exclude from
        update.
        """

        if not self.key:
            return

        ReferenceField = fields.ReferenceField

        _exclude = list(self.get_key_name())
        for e in exclude:
            class_field = getattr(self.__class__, e)
            if isinstance(class_field, ReferenceField):
                _exclude.extend(list(class_field.get_fk_columns().keys()))
            else:
                _exclude.append(e)

        data = {
            p: getattr(self, p)
            for p in self.get_columns()
            if p not in _exclude
        }

        table = self.get_table()
        stmt = table.update().values(**data)
        for col_name, col_value in zip(self.get_key_name(), self.key):
            stmt = stmt.where(table.columns[col_name] == col_value)

        return self.metadata.bind.connect().execute(stmt)

    def update_or_save(self):
        """ Update or insert new record

        If primary key is present the function will try to update
        if it cannot update it will insert new record. If primary
        key is not present the function will insert new record.
        """
        if self.key:
            result = self.update()
            if result.rowcount:
                return result

        return self.save()

    def delete(self):
        key = self.key
        key_name = self.get_key_name()
        table = self.get_table()

        conn = self.metadata.bind.connect()

        stmt = table.delete()
        for col_name, col_value in zip(key_name, key):
            stmt = stmt.where(table.columns[col_name] == col_value)

        result = conn.execute(stmt)
        if result.rowcount:
            return result
