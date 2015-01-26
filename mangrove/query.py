import sqlalchemy
from mangrove import connection


class SelectStatement():
    """ Manages the underlying sqlalchemy select statement
    """

    def __init__(self, columns=[], funcs=[]):
        """ Constructor

        :param list columns: A list of columns that need to be part of
            this select statement.
        :param list funcs: A list of `sqlalchemy.funcs`
        """
        self.stmt = sqlalchemy.select(columns or funcs)

    def execute(self, *args, **kwargs):
        """ Executes the statement using the default connection
        """
        return connection.get_connection().execute(self.stmt)

    def where(self, *args, **kwargs):
        """  Adds where clause to the select statement

        class Person(Model):
            name = StringField()

        Person.select().where(Person.name=='John Doe')
        -or-
        Person.select().where('name = "John Doe"')
        -or-
        SelectStatement(Person).where(Person.name=='John Doe')
        -or-
        SelectStatement(Person).where('name = "John Doe"')
        """

        self.stmt = self.stmt.where(*args, **kwargs)
        return self


class Query(SelectStatement):
    """ Adds functionality to fetch rows to `SelectStatement`
    """

    def __init__(self, model, columns=[]):
        """ Constructor

        :param `models.Model` model: The model on which to run the query
        :param list columns: Columns which will be included in the query
        """
        self.model = model
        columns = columns or [model.get_table()]
        super(Query, self).__init__(columns=columns)

    def __iter__(self):
        """ Allows query object to be iterated over
        """
        for row in self.execute():
            yield self.model(**dict(row))

    def order_by(self, *args, **kwargs):
        """ Adds orderby clause to the query

        .. code
        >>> Query(Model).order_by('id').fetchall()
        >>> Query(Model).order_by(Person.id).fetchall()
        >>> Query(Model).order_by('-id').fetchall()
        >>> Query(Model).order_by(-Person.id).fetchall()
        """
        self.stmt = self.stmt.order_by(*args, **kwargs)
        return self

    def fetchall(self, *multiparams, **params):
        """ Return all rows as list
        """
        items = self.execute(*multiparams, **params).fetchall()
        return [self.model(**dict(row)) for row in items]

    def fetchmany(self, size=None, *multiparams, **params):
        """ Return a particular number of rows

        :param int size: The number of rows which should be returned
        """
        items = self.execute(*multiparams, **params).fetchmany(size)
        return [self.model(**dict(row)) for row in items]

    def fetchone(self, *multiparams, **params):
        """ Return one row
        """
        item = self.execute(*multiparams, **params).fetchone()
        try:
            return self.model(**dict(item))
        except TypeError:
            return None

    def first(self, *multiparams, **params):
        """ Return first row
        """
        item = self.execute(*multiparams, **params).first()
        try:
            return self.model(**dict(item))
        except TypeError:
            return None


class Counter(SelectStatement):
    """ Adds functionality to count rows to `SelectStatement`
    """
    def __init__(self, model):
        super(Counter, self).__init__(funcs=[sqlalchemy.func.count()])
        self.stmt = self.stmt.select_from(model.get_table())

    def scalar(self):
        """ Returns scalar value of the count
        """
        return self.execute().scalar()
