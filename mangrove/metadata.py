import sqlalchemy

from mangrove import engine


class MetaData():

    _metadata = None

    @classmethod
    def setup_metadata(cls, engine):
        cls._metadata = sqlalchemy.MetaData(engine)
        cls._metadata.reflect()

    @classmethod
    def get_metadata(cls):
        if cls._metadata is not None:
            # return the cached metadata
            return cls._metadata

        ngin = engine.Engine.get_engine()
        cls.setup_metadata(ngin)
        return cls._metadata
