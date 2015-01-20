import tests

from mangrove import models
from mangrove import fields
from mangrove import connection


class AddModelTestCase(tests.BaseTestCase):

    def test_add_model(self):
        class Person(models.Model):
            name = fields.StringField()

        connection._connection = None
        connection.add_model(Person)
        self.assertIn('Person', connection._metadata)

        conn = connection.SqliteConnection()
        connection.install_connection(conn)

        class Person2(models.Model):
            name = fields.StringField()

        self.assertIn('Person2', connection._metadata)

    def test_install(self):
        connection._connection = None
        sqlite_conn = connection.SqliteConnection()
        self.assertIs(connection._connection, None)
        connection.install_connection(sqlite_conn)
        self.assertIsNot(connection._connection, None)

    def test_constructor_init(self):
        class Person(models.Model):
            name = fields.StringField()

        sqlite_conn = connection.SqliteConnection()
        self.assertIn('Person', connection._metadata)


class MultipleConnectionTestCase(tests.BaseTestCase):
    def test_multiple_connection(self):
        conn1 = connection.get_connection()

        class Person(models.Model):
            name = fields.StringField()

        Person(name='Umair').save()
        self.assertEqual(len(Person.select().fetchall()), 1)
        conn2 = connection.SqliteConnection()
        connection.install_connection(conn2)
        self.assertEqual(len(Person.select().fetchall()), 0)
        Person(name='Khan').save()
        persons = Person.select().fetchall()
        self.assertEqual(len(persons), 1)
        self.assertEqual(persons[0].name, 'Khan')

        connection.install_connection(conn1)
        persons = Person.select().fetchall()
        self.assertEqual(len(persons), 1)
        self.assertEqual(persons[0].name, 'Umair')
