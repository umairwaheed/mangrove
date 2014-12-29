import unittest

import engine
engine.Engine.init_engine(echo=False)

import metadata


class WebEggDBTestCase(unittest.TestCase):

    def clean_database(self):
        _engine = engine.Engine.get_engine()
        _metadata = metadata.MetaData.get_metadata()
        for table in _metadata.sorted_tables:
            _engine.execute(table.delete())

    def setUp(self):
        self.clean_database()
