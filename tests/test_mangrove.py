import tests
from mangrove import models
from mangrove import fields
from mangrove import connection


class DateTimeFieldTestCase(tests.BaseTestCase):
    def test_datetime_field(self):
        class Car(models.Model):
            name = fields.StringField()
            creation_time = fields.DateTimeField()

        import datetime

        creation_time = datetime.datetime.now()
        car = Car(name="Foo", creation_time=creation_time)
        car.save()

        db_car = Car.select().fetchone()

        self.assertEqual(creation_time, car.creation_time)
        self.assertEqual(creation_time, db_car.creation_time)


class ReferenceFieldTestCase(tests.BaseTestCase):

    def test_reference_field(self):
        Model = models.Model
        StringField = fields.StringField
        ReferenceField = fields.ReferenceField

        class Shape(Model):
            name = StringField()

        class Point(Model):
            position = StringField()
            shape = ReferenceField(Shape)

        rect = Shape(name='rectangle')
        rect.save()

        p1 = Point(position='1', shape=rect)
        p1.save()

        p = Point.select().fetchone()
        cache_name = 'cache_fk_shape'
        self.assertFalse(hasattr(p, cache_name))
        self.assertEqual(p.position, '1')
        self.assertEqual(p.shape.name, 'rectangle')
        self.assertTrue(hasattr(p, cache_name))

    def test_name_of_constraint(self):
        Model = models.Model
        StringField = fields.StringField
        ReferenceField = fields.ReferenceField

        class Parent(Model):
            name = StringField()

        class Child(Model):
            name = StringField()
            parent = ReferenceField(Parent)

        self.assertEqual(Child.parent.name, 'fk_parent')

        class Child(Model):
            name = StringField()
            parent = ReferenceField(Parent)

        self.assertEqual(Child.parent.name, 'fk_parent')


class ModelTestCase(tests.BaseTestCase):

    def test_abstract_model(self):

        class Shape(models.Model):
            abstract = True
            name = fields.StringField()

        class Rectangle(Shape):
            abstract = False

        self.assertIs(connection.get_table(Shape), None)
        self.assertIsNot(connection.get_table(Rectangle), None)
        self.assertIn('name', Rectangle.get_columns())

    def test_delete(self):

        class Person(models.Model):
            name = fields.StringField()
            age = fields.IntegerField()

        p = Person(name='John Doe', age=32)
        p.save()

        self.assertEqual(len(list(Person.select())), 1)

        p.delete()

        self.assertEqual(len(list(Person.select())), 0)

    def test_save(self):

        class Person(models.Model):
            name = fields.StringField()
            age = fields.IntegerField()

        p = Person(name='Jon Doe', age=32)
        p.save()

        db_person = list(Person.select())
        self.assertEqual(len(db_person), 1)

        db_person = db_person[0]
        self.assertEqual(db_person.name, 'Jon Doe')
        self.assertEqual(db_person.age, 32)

        import sqlalchemy.exc
        p = Person(id=db_person.id)
        try:
            p.save()
        except sqlalchemy.exc.IntegrityError as e:
            # we cannot insert save already inserted record.
            # Use update
            pass

        # Insert record for which we already know the primary key
        class Country(models.Model):
            name = fields.StringField(primary_key=True)
            population = fields.IntegerField()

        country = Country(name='Pakistan', population=200*10**6)
        country.save()

        self.assertEqual(len(list(Country.select())), 1)

        try:
            country.save()
        except sqlalchemy.exc.IntegrityError as e:
            pass

    def test_update(self):

        class Person(models.Model):
            name = fields.StringField()
            age = fields.IntegerField()

        p = Person(name='Jon Doe', age=32)
        p.save()

        p.name = 'Umair Khan'
        result = p.update()

        self.assertEqual(Person.select().fetchone().name, 'Umair Khan')
        self.assertEqual(len(list(Person.select())), 1)
        self.assertEqual(result.rowcount, 1)

        result = Person(id=100, name='Jojen').update()
        self.assertEqual(len(list(Person.select())), 1)
        self.assertEqual(result.rowcount, 0)

        Person(name='Jon Doe', age=50).save()

        p.name = 'Some New Name'
        p.age = 100
        p.update()

        name = set([p.name for p in Person.select()])
        self.assertEqual(len(name), 2)

        class Parent(models.Model):
            name = fields.StringField()

        class Child(models.Model):
            name = fields.StringField()
            parent = fields.ReferenceField(Parent)

        p = Parent(name='Jon')
        p.save()

        c = Child(name='Doe', parent=p)
        c.save()

        Child(name='Boo', id=1).update()
        self.assertEqual(Child.select().fetchone().parent, None)

        c.update()
        Child(name='Boo', id=1).update(exclude=['parent'])
        child = Child.select().fetchone()
        self.assertEqual(child.name, 'Boo')
        self.assertEqual(child.fk_parent_id, 1)
        self.assertTrue(child.parent is not None)

    def test_update_or_save(self):
        class Person(models.Model):
            name = fields.StringField()

        result = Person(name='Jon Doe').update_or_save()
        self.assertEqual(result.rowcount, 1)
        self.assertEqual(len(list(Person.select())), 1)

        result = Person(id=1, name='Umair').update_or_save()
        self.assertEqual(result.rowcount, 1)
        self.assertEqual(len(list(Person.select())), 1)

    def test_get_by_key(self):
        class Person(models.Model):
            name = fields.StringField()
            age = fields.IntegerField()

        p = Person(name='John Doe', age=32)
        p.save()

        p2 = Person.get_by_key({Person.id: p.key[0]})
        self.assertEqual(p2.id, p.id)
        self.assertEqual(p2.name, p.name)
        self.assertEqual(p2.age, p.age)

    def test_count(self):
        class Person(models.Model):
            name = fields.StringField()
            age = fields.IntegerField()

        for i in range(10):
            Person(name='umair', age=i).save()

        Person(name='khan', age=11).save()

        self.assertEqual(Person.get_count().scalar(), 11)
        self.assertEqual(
            Person.get_count().where(Person.name == 'umair').scalar(), 10)
        self.assertEqual(Person.get_count().where(Person.age == 1).scalar(), 1)

    def test_ordering(self):
        class Person(models.Model):
            name = fields.StringField()
            age = fields.IntegerField()

        for i in range(10):
            Person(name='umair', age=i).save()

        self.assertEqual(Person.select().order_by('age').first().age, 0)
        self.assertEqual(Person.select().order_by('-age').first().age, 9)
