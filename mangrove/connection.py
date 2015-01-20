import sqlalchemy


# Global connection
_connection = None

# Metadata
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

    def execute(self, statement):
        """ Execute statement on this connection
        """
        return self._engine.connect().execute(statement)

    def drop_all(self, *args, **kwargs):
        """ Drop all tabls from DB and metadata
        """
        _metadata.drop_all(self._engine, *args, **kwargs)
        _metadata.clear()


class SqliteConnection(Connection):
    """ Create connection to Sqlite DB
    """
    def __init__(self, dbpath=":memory:", **kwargs):
        connection_string = "sqlite:///%s" % dbpath
        super(SqliteConnection, self).__init__(connection_string, **kwargs)


def install_connection(connection):
    """ Install as default connection
    """
    global _connection
    _connection = connection


def add_model(model_cls):
    """ Add model to the DB
    """
    table_name = model_cls.__name__

    # add columns and constraints to tbe table
    if table_name not in _metadata.tables and not model_cls.abstract:
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


def get_table(model_cls):
    add_model(model_cls)
    return _metadata.tables.get(model_cls.__name__)


def get_connection():
    return _connection


# Install default connection
install_connection(SqliteConnection())
