import sys
import json
import sqlalchemy
import collections

from mangrove import query
from mangrove import fields
from mangrove import exceptions
from mangrove import connection


if sys.version_info < (3, 0):
    import py2_base as base
else:
    from mangrove import py3_base as base


class Model(base.ModelBase):
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

        return connection.get_table(cls)

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
        """ Create query over this model

        :param list columns: List of columns that need to be returned

        :returns: `query.Counter` object.
        """
        return query.Query(cls, columns=columns)

    @classmethod
    def count(cls):
        """ Count records

        :returns: `query.Counter` object.
        """
        return query.Counter(cls)

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
        result = connection.get_connection().execute(stmt)

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

        return connection.get_connection().execute(stmt)

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

        stmt = table.delete()
        for col_name, col_value in zip(key_name, key):
            stmt = stmt.where(table.columns[col_name] == col_value)

        result = connection.get_connection().execute(stmt)
        if result.rowcount:
            return result
