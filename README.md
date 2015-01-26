# Mangrove - An SQLAlchemy Wrapper
Mangrove is a simplification layer over [SQLAlchemy](www.sqlalchemy.org/), it
takes care of the boilerplate involved in using SQLAlchemy.

## Python
Supports both Python 2 and Python 3

## Examples
Create model using:
```
import models
class Person(models.Model):
    name = fields.StringField()
    age = fields.IntegerField()
```

Save entity using:
```
person = Person()
person.name = "Foobar"
person.age = 30
person.save()
```

Retrieve entity using:
```
for person in Person.select():
    print(person)
```

Filter queries:
```
for person in Person.select().where(Person.name == "Foobar"):
    print(person)
```

Count rows:
```
print(Person.count().scalar())
print(Person.count().where(Person.name == 'Foobar').scalar()
```

Ordering
```
Person.select().order_by('name')
Person.select().order_by(Person.name)
Person.select().order_by('-name')
Person.select().order_by(-Person.name)
```


## TODOs
- Add API reference.
- Add tutorial
- Add detailed documentation
- Integrate with ReadTheDocs
- Integrate with Travis CI.
- Add test coverage report.
- Add CHANGELOG file
