__author__ = "umair.waheed@gmail.com(Umair Khan)"

import sys

try:
  from setuptools import setup, find_packages
except ImportError:
  import distribute_setup
  distribute_setup.use_setuptools()
  from setuptools import setup, find_packages


includes = ['sqlalchemy',
            'mock']
if sys.version_info[0] == 2:
    #includes.append('pysqlite')
    pass


setup(
    name='mangrove',
    version='0.1',
    packages=find_packages(),
    author='Umair Khan',
    author_email='umair.waheed@gmail.com',
    description='SqlAlchemy Wrapper',
    test_suite='test',
    install_requires=includes,
    url='http://www.github.com/umairwaheed/mangrove',
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Development Status :: 3 - Alpha', 
    ],
    keywords="sqlalchemy wrapper orm python"
)
