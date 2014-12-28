import webeggdb.tests
import webeggdb.models
import webeggdb.fields


class ReferenceFieldTestCase(webeggdb.tests.WebEggDBTestCase):

    def test_reference_field(self):
        Model = webeggdb.models.Model
        StringField = webeggdb.fields.StringField
        ReferenceField = webeggdb.fields.ReferenceField

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
        Model = webeggdb.models.Model
        StringField = webeggdb.fields.StringField
        ReferenceField = webeggdb.fields.ReferenceField

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


class ModelTestCase(webeggdb.tests.WebEggDBTestCase):

    def test_abstract_model(self):

        class Shape(webeggdb.models.Model):
            abstract = True
            name = webeggdb.fields.StringField()

        class Rectangle(Shape):
            abstract = False

        metadata = Shape.metadata
        self.assertFalse('Shape' in metadata.tables)
        self.assertTrue('Rectangle' in metadata.tables)
        self.assertIn('name', Rectangle.get_columns())

    def test_delete(self):

        class Person(webeggdb.models.Model):
            name = webeggdb.fields.StringField()
            age = webeggdb.fields.IntegerField()

        p = Person(name='John Doe', age=32)
        p.save()

        self.assertEqual(len(list(Person.select())), 1)

        p.delete()

        self.assertEqual(len(list(Person.select())), 0)

    def test_save(self):

        class Person(webeggdb.models.Model):
            name = webeggdb.fields.StringField()
            age = webeggdb.fields.IntegerField()

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
        class Country(webeggdb.models.Model):
            name = webeggdb.fields.StringField(primary_key=True)
            population = webeggdb.fields.IntegerField()

        country = Country(name='Pakistan', population=200*10**6)
        country.save()

        self.assertEqual(len(list(Country.select())), 1)

        try:
            country.save()
        except sqlalchemy.exc.IntegrityError as e:
            pass

    def test_update(self):

        class Person(webeggdb.models.Model):
            name = webeggdb.fields.StringField()
            age = webeggdb.fields.IntegerField()

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

        class Parent(webeggdb.models.Model):
            name = webeggdb.fields.StringField()

        class Child(webeggdb.models.Model):
            name = webeggdb.fields.StringField()
            parent = webeggdb.fields.ReferenceField(Parent)

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
        class Person(webeggdb.models.Model):
            name = webeggdb.fields.StringField()

        result = Person(name='Jon Doe').update_or_save()
        self.assertEqual(result.rowcount, 1)
        self.assertEqual(len(list(Person.select())), 1)

        result = Person(id=1, name='Umair').update_or_save()
        self.assertEqual(result.rowcount, 1)
        self.assertEqual(len(list(Person.select())), 1)

    def test_get_by_key(self):
        class Person(webeggdb.models.Model):
            name = webeggdb.fields.StringField()
            age = webeggdb.fields.IntegerField()

        p = Person(name='John Doe', age=32)
        p.save()

        p2 = Person.get_by_key({Person.id: p.key[0]})
        self.assertEqual(p2.id, p.id)
        self.assertEqual(p2.name, p.name)
        self.assertEqual(p2.age, p.age)
