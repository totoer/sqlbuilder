#-*- coding: utf-8 -*-

import sqlbuilder
from setuptools import setup, find_packages


setup(
    name="sqlbuilder",
    version=sqlbuilder.__version__,
    url='https://github.com/totoer/sqlbuilder/',
    author='Alexey Protsenko',
    author_email="totoeru@gmail.com",
    description="Simple sql bulder",
    license="Apache License",
    packages=find_packages())
