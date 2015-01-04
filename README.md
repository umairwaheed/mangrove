# Mangrove - An SQLAlchemy Wrapper
Mangrove is a simplification layer over [SQLAlchemy](www.sqlalchemy.org/), it
takes care of the boilerplate involved in using SQLAlchemy.

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

## TODOs
- Add API reference.
- Add tutorial
- Add detailed documentation
- Integrate with ReadTheDocs
- Integrate with Travis CI.
- Add test coverage report.
- Add CHANGELOG file
