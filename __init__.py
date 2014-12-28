"""
WebEggDB module is a simple syntax sugar over sqlalchemy to make it look
like a ActiveRecord style ORM.
"""


__author__ = "Umair Waheed Khan"
__version__ = "0.0.0"


# Global configuration of db module
config = {
    'debug': True,
    'engine': {
        'connection_string': 'sqlite:///:memory:',
        'echo': True,
    },
}
