import sqlalchemy


# Global connection
_connection = None

# Tables
_tables = {}


class Connection(object):
    """ Base class for all connections

    Handles metadata and engine
    """
    def __init__(self, connection_string, **kwargs):
        self._engine = sqlalchemy.create_engine(connection_string, **kwargs)
        self._metadata = sqlalchemy.MetaData(self._engine)
        self._metadata.reflect()
        self.init_tables()

    def init_tables(self, tables=None):
        """ Create the DB tables

        Creates all the columns of the table, adds a primary key
        column if necessary and creates the table in the database.

        :param dict tables: The dictionary containing tables you want to
        initialize
        """

        tables = tables or _tables
        metadata = self._metadata
        SQLTable = sqlalchemy.Table
        for table_name, model_cls in tables.items():
            columns = model_cls.get_columns()
            constraints = model_cls.get_constraints()
            # add columns and constraints to tbe table
            if table_name not in metadata.tables:
                table = SQLTable(table_name, metadata)
                try:
                    for name, column in columns.items():
                        column = column.copy()
                        table.append_column(column)

                    for name, constraint in constraints.items():
                        if hasattr(constraint, 'parent'):
                            constraint = constraint.copy()

                        table.append_constraint(constraint)

                except Exception:
                    # in case of exception remove the table from the metadata
                    metadata.remove(table)
                    raise # re-raise the exception

        metadata.create_all()


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
    model_name = model_cls.__name__
    if model_name in _tables:
        return

    new_table = {model_name: model_cls}
    _tables.update(new_table)
    if _connection is not None:
        _connection.init_tables(new_table)
