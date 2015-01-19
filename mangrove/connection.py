import sqlalchemy


# Global connection
_connection = None

# Tables
_metadata = sqlalchemy.MetaData()


class Connection(object):
    """ Base class for all connections

    Handles metadata and engine
    """
    def __init__(self, connection_string, **kwargs):
        engine = sqlalchemy.create_engine(connection_string, **kwargs)
        self._engine = engine
        _metadata.reflect(engine)
        _metadata.create_all(engine)


class SqliteConnection(Connection):
    """ Create connection to Sqlite DB
    """
    def __init__(self, dbpath=":memory:", **kwargs):
        connection_string = "sqlite:///%s" % dbpath
        super(SqliteConnection, self).__init__(connection_string, **kwargs)


def install_connection(connection):
    global _connection
    _connection = connection


def add_model(model_cls):
    table_name = model_cls.__name__

    # add columns and constraints to tbe table
    if table_name not in _metadata.tables:
        table = sqlalchemy.Table(table_name, _metadata)
        try:
            for name, column in model_cls.get_columns().items():
                column = column.copy()
                table.append_column(column)

            for name, constraint in model_cls.get_constraints().items():
                if hasattr(constraint, 'parent'):
                    constraint = constraint.copy()

                table.append_constraint(constraint)

        except Exception:
            # in case of exception remove the table from the _metadata
            _metadata.remove(table)
            raise # re-raise the exception
        else:
            if _connection is not None:
                _metadata.create_all(_connection._engine, tables=[table])
