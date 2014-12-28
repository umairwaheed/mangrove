import unittest

import webeggdb.engine
webeggdb.engine.Engine.init_engine(echo=False)

import webeggdb.metadata


class WebEggDBTestCase(unittest.TestCase):

    def clean_database(self):
        engine = webeggdb.engine.Engine.get_engine()
        metadata = webeggdb.metadata.MetaData.get_metadata()
        for table in metadata.sorted_tables:
            engine.execute(table.delete())

    def setUp(self):
        self.clean_database()
