from mangrove import fields
from mangrove import exceptions
from mangrove import connection


class MetaCls(type):
    """Initialize the class attributes

    The meta class is used to assign names to the fields of the model
    """
    def __new__(metacls, name, bases, namespace, **kwargs):
        cls = type.__new__(metacls, name, bases, namespace, **kwargs)

        if cls.abstract:
            return cls

        try:
            constraints = cls.get_constraints()
            columns = cls.get_columns()
        except AttributeError:
            # This is a top most base class
            return cls

        if not len(constraints) and not len(columns):
            # Empty model
            return cls

        # Add foreign key columns to the model
        for name, constraint in constraints.items():
            # we need to assign name every time class is created.
            constraint.name = constraint.name or constraint.apply_prefix(name)
            for name, column in constraint.get_fk_columns().items():
                setattr(cls, name, column)

        # Need to call the function because `column` is outdated
        # because foreign key column may have been added by the above
        # code
        columns = cls.get_columns()
        for name, column in columns.items():
            # `column.name` is the name assigned by the user in the
            # constructor Field(name=[]...), it is given preference.
            column.name = column.name or name

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

        connection.add_model(cls)
        return cls
