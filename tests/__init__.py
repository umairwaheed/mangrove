import unittest
from mangrove import connection


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        connection.get_connection().drop_all()
