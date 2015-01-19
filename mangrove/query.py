import sqlalchemy
from mangrove import connection


class Query():

    def __init__(self, model, columns=[]):
        self.model = model
        columns = columns or [model.get_table()]
        self.stmt = sqlalchemy.select(columns)

    def __iter__(self):
        for row in self.execute():
            yield self.model(**dict(row))

    def execute(self, *args, **kwargs):
        return connection.get_connection().execute(self.stmt)

    def where(self, *args, **kwargs):
        """

        class Person(Model):
            name = StringField()

        Person.select().where(Person.name=='John Doe')
        -or-
        Person.select().where('name = "John Doe"')
        -or-
        Query(Person).where(Person.name=='John Doe')
        -or-
        Query(Person).where('name = "John Doe"')
        """

        self.stmt = self.stmt.where(*args, **kwargs)
        return self

    def fetchall(self, *multiparams, **params):
        items = self.execute(*multiparams, **params).fetchall()
        return [self.model(**dict(row)) for row in items]

    def fetchmany(self, size=None, *multiparams, **params):
        items = self.execute(*multiparams, **params).fetchmany(size)
        return [self.model(**dict(row)) for row in items]

    def fetchone(self, *multiparams, **params):
        item = self.execute(*multiparams, **params).fetchone()
        try:
            return self.model(**dict(item))
        except TypeError:
            return None

    def first(self, *multiparams, **params):
        item = self.execute(*multiparams, **params).first()
        try:
            return self.model(**dict(item))
        except TypeError:
            return None
