import sqlalchemy

import settings


class Engine():

    _engine = None

    @classmethod
    def init_engine(cls, connection_string=None, echo=None):
        if cls._engine is not None:
            # already initialized
            return

        connection_string = (
            connection_string if connection_string is not None else
            settings.config['engine']['connection_string']
        )

        echo = echo if echo is not None else settings.config['engine']['echo']
        cls._engine = sqlalchemy.create_engine(connection_string, echo=echo)

    @classmethod
    def get_engine(cls):
        if cls._engine is not None:
            # return the cached engine
            return cls._engine

        cls.init_engine(**settings.config['engine'])
        return cls._engine
