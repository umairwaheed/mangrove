import sqlalchemy

from mangrove import fields
from mangrove import metadata
from mangrove import exceptions


class Meta(type):
    """Initialize the class attributes

    The meta class is used to assign names to the fields of the model
    """
    def __new__(metacls, name, bases, namespace, **kwargs):
        cls = type.__new__(metacls, name, bases, namespace, **kwargs)

        if cls.__name__ == "ModelBase":
            return cls

        # Add foreign key columns to the model
        for name, constraint in cls.get_constraints().items():
            # we need to assign name every time class is created.
            constraint.name = constraint.name or constraint.apply_prefix(name)
            for name, column in constraint.get_fk_columns().items():
                setattr(cls, name, column)

        for name, column in cls.get_columns().items():
            # `column.name` is the name assigned by the user in the
            # constructor Field(name=[]...), it is given preference.
            column.name = column.name or name

        if cls.__name__ != "Model" and not cls.abstract:
            metacls._init_table(cls)

        return cls

    @staticmethod
    def _init_table(cls):
        """Initialize the underlying SQLAlchemy table

        Creates all the columns of the table, adds a primary key
        column if necessary and creates the table in the database.
        """

        metadata = cls.metadata
        columns = cls.get_columns()

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

        # add columns and constraints to tbe table
        table_name = cls.__name__
        if table_name not in metadata.tables:
            table = sqlalchemy.Table(cls.__name__, metadata)
            try:
                for name, column in columns.items():
                    column = column.copy()
                    table.append_column(column)

                for name, constraint in cls.get_constraints().items():
                    if hasattr(constraint, 'parent'):
                        constraint = constraint.copy()

                    table.append_constraint(constraint)

                metadata.create_all(tables=[table])
            except Exception:
                # in case of exception remove the table from the metadata
                metadata.remove(table)
                raise # re-raise the exception
