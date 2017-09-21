#
# LSST Data Management System
# Copyright 2017 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#


import numpy
import os
import tempfile
import shutil
import sqlalchemy
import unittest

import lsst.utils.tests
from lsst.daf.fmt.mysql import SqlStorage
import lsst.daf.persistence as dafPersist
from dafFmtMysqlTestUtils import make_afw_base_catalog, columnSchema


def setup_module(module):
    lsst.utils.tests.init()


ROOT = os.path.abspath(os.path.dirname(__file__))


class MyMapper(dafPersist.Mapper):

    def __init__(self, root, *args, **kwargs):
        self.storage = dafPersist.Storage.makeFromURI(root)
        dafPersist.Mapper.__init__(root, *args, **kwargs)

    def map_table(self, dataId, write):
        loc = dafPersist.ButlerLocation(pythonType=lsst.afw.table.BaseCatalog,
                                        cppType=None,
                                        storageName=None,
                                        locationList=['testname'],
                                        dataId={},
                                        mapper=self,
                                        storage=self.storage)
        return loc


class TableIoTestCase(unittest.TestCase):

    def setUp(self):
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix="TableIoTestCase-")

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    def test_create_false(self):
        """Test that NoRepositoryAtRoot is raised if no repository exists and create is False"""
        dbLocation = os.path.join('sqlite:///', os.path.relpath(self.testDir), 'test.db')
        with self.assertRaises(dafPersist.NoRepositroyAtRoot):
            SqlStorage(dbLocation, create=False)

    def test_write_read(self):
        """Test that Butler can write a BaseCatalog to an sqlite database, and that the database can be read
        by sqlalchemy and compares equal to the original.
        """
        cat_expected = make_afw_base_catalog(
            [columnSchema('a', numpy.int64, 'a'), columnSchema('b', numpy.float64, 'a')],
            ((12345, 1.2345), (4321, 4.123)))
        dbLocation = os.path.join('sqlite:///', os.path.relpath(self.testDir), 'test.db')
        butler = dafPersist.Butler(outputs={'cfgRoot': self.testDir, 'root': dbLocation, 'mapper': MyMapper})

        # Test writing first
        butler.put(cat_expected, 'table')
        del butler

        # Test reading back with raw object
        engine = sqlalchemy.create_engine(dbLocation)
        rows = engine.execute("select a, b from testname")
        # FIXME: Hopefully tables implement __eq__ in the future
        self._compare_table(cat_expected, rows)

        # Test reading back via butler.get
        butler = dafPersist.Butler(inputs=self.testDir)
        cat_reloaded = butler.get('table')
        self._compare_table(cat_expected, cat_reloaded)

    def test_append(self):
        """Test that writing a base catalog to the same location appends the rows to the existing table."""
        cat1 = make_afw_base_catalog(
            [columnSchema('a', numpy.int64, 'a'), columnSchema('b', numpy.float64, 'a')],
            ((12345, 1.2345), (4321, 4.123)))
        dbLocation = os.path.join('sqlite:///', os.path.relpath(self.testDir), 'test.db')
        butler = dafPersist.Butler(outputs={'cfgRoot': self.testDir, 'root': dbLocation, 'mapper': MyMapper})
        butler.put(cat1, 'table')
        del butler

        # add more data
        cat2 = make_afw_base_catalog(
            [columnSchema('a', numpy.int64, 'a'), columnSchema('b', numpy.float64, 'a')],
            ((42, 4.2), (24, 2.4)))
        dbLocation = os.path.join('sqlite:///', os.path.relpath(self.testDir), 'test.db')
        butler = dafPersist.Butler(outputs={'cfgRoot': self.testDir, 'root': dbLocation, 'mapper': MyMapper})
        butler.put(cat2, 'table')
        del butler

        cat_expected = make_afw_base_catalog(
            [columnSchema('a', numpy.int64, 'a'), columnSchema('b', numpy.float64, 'a')],
            ((12345, 1.2345), (4321, 4.123), (42, 4.2), (24, 2.4)))

        # Test reading back with raw object
        engine = sqlalchemy.create_engine(dbLocation)
        rows = engine.execute("select a, b from testname")
        # FIXME: Hopefully tables implement __eq__ in the future
        self._compare_table(cat_expected, rows)

        # Test reading back via butler.get
        butler = dafPersist.Butler(outputs={'root': self.testDir, 'mode': 'rw'})
        cat_reloaded = butler.get('table')
        self._compare_table(cat_expected, cat_reloaded)

    def test_append_extra_field(self):
        """Test that if a catalog with an extra column is appended to an existing catalog an exception is
        raised."""
        cat1 = make_afw_base_catalog(
            [columnSchema('a', numpy.int64, 'a'),
             columnSchema('b', numpy.float64, 'b')],
            ((12345, 1.2345), (4321, 4.123)))
        dbLocation = os.path.join('sqlite:///', os.path.relpath(self.testDir), 'test.db')
        butler = dafPersist.Butler(outputs={'cfgRoot': self.testDir, 'root': dbLocation, 'mapper': MyMapper,
                                   'mode': 'rw'})
        butler.put(cat1, 'table')

        cat2 = make_afw_base_catalog(
            [columnSchema('a', numpy.int64, 'a'),
             columnSchema('b', numpy.float64, 'b'),
             columnSchema('c', numpy.int64, 'c')],
            ((42, 4.2, 123), (24, 2.4, 234)))
        with self.assertRaises(sqlalchemy.exc.OperationalError):
            butler.put(cat2, 'table')

    @unittest.expectedFailure
    def test_reorder_fields(self):
        """Test that if a catalog with an extra column is appended to an existing catalog an exception is
        raised."""
        cat1 = make_afw_base_catalog(
            [columnSchema('a', numpy.int64, 'a'),
             columnSchema('b', numpy.float64, 'b')],
            ((12345, 1.2345), (4321, 4.123)))
        dbLocation = os.path.join('sqlite:///', os.path.relpath(self.testDir), 'test.db')
        butler = dafPersist.Butler(outputs={'cfgRoot': self.testDir, 'root': dbLocation, 'mapper': MyMapper,
                                   'mode': 'rw'})
        butler.put(cat1, 'table')

        cat2 = make_afw_base_catalog(
            [columnSchema('a', numpy.float64, 'a'),
             columnSchema('b', numpy.int64, 'b')],
            ((4.2, 42,), (2.4, 24)))
        with self.assertRaises(sqlalchemy.exc.OperationalError):
            butler.put(cat2, 'table')

    def test_no_cfg_root_raises_with_output_repo(self):
        """Right now we don't support writing a RepositoryCfg direcetly into a database. Test that an
        exception is raised if a cfgRoot is not specified for the database repository."""
        dbLocation = os.path.join('sqlite:///', os.path.relpath(self.testDir), 'test.db')
        with self.assertRaises(RuntimeError):
            butler = dafPersist.Butler(outputs={'root': dbLocation, 'mapper': MyMapper})
            del butler

    def test_no_cfg_root_raises_with_input_repo(self):
        """Right now we don't support reading a RepositoryCfg direcetly into a database. Test that an
        exception is raised if a cfgRoot is not specified for the database repository."""
        # create a database repo with a cfg on the filesystem:
        dbLocation = os.path.join('sqlite:///', os.path.relpath(self.testDir), 'test.db')
        butler = dafPersist.Butler(outputs={'cfgRoot': self.testDir, 'root': dbLocation, 'mapper': MyMapper})
        del butler
        # pass the dbLocation for root (instead of the cfgRoot) and check that it raises an exception.
        # with self.assertRaises(RuntimeError):
        with self.assertRaises(RuntimeError):
            butler = dafPersist.Butler(inputs=dbLocation)
            del butler

    def _compare_table(self, afw_cat, rows):
        columns = [str(i.field.getName()) for i in afw_cat.schema]
        for cat1_row, cat2_row in zip(afw_cat, rows):
            for column in columns:
                self.assertEqual(cat1_row[column], cat2_row[column])

    def test_get_mapper_class(self):
        """Test that SqlStorage raises if an sqlite root is passed and defers to posix storage if a posix
        root is passed."""
        dbLocation = os.path.join('sqlite:///', os.path.relpath(self.testDir), 'test.db')
        butler = dafPersist.Butler(outputs={'cfgRoot': self.testDir, 'root': dbLocation, 'mapper': MyMapper})
        self.assertIsInstance(butler.getMapperClass(self.testDir), type(MyMapper))
        with self.assertRaises(RuntimeError):
            butler.getMapperClass(dbLocation)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
