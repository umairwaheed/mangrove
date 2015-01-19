import tests

from mangrove import models
from mangrove import fields
from mangrove import connection


class Person(models.Model):
    name = fields.StringField()


class AddModelTestCase(tests.BaseTestCase):

    def test_none(self):
        connection._connection = None
        connection.add_model(Person)
        self.assertIn('Person', connection._tables)

    def test_install(self):
        sqlite_conn = connection.SqliteConnection()
        print(connection._connection)
        print(sqlite_conn._metadata.tables)
        self.assertNotIn('Person', sqlite_conn._metadata)
        connection.install_connection(sqlite_conn)
        connection.add_model(Person)
        self.assertIn('Person', sqlite_conn._metadata)

    def test_constructor_init(self):
        pass

    def test_reflect(self):
        sqlite_conn = connection.SqliteConnection()
        connection.install_connection(sqlite_conn)
        connection.add_model(Person)
