#!/usr/bin/env python

#
# See COPYRIGHT file at the top of the source tree
#

from __future__ import print_function
from contextlib import closing
import lsst.afw.table
import lsst.daf.persistence as dafPersist
from lsst.daf.fmt.mysql import AfwTableSqlFormatter, MySqlStorage
import lsst.utils.tests
import MySQLdb
import unittest
import os

# Define the root of the tests relative to this file
ROOT = os.path.abspath(os.path.dirname(__file__))


class MapperForTest():

    def __init__(*args, **kwargs):
        pass


class TestMySqlStorage(unittest.TestCase):
    """A test case for the AFWTable Sql Formatter"""

    def setUp(self):
        self.host = 'localhost'
        self.port = 3306
        self.removeDatabases = []

    def tearDown(self):
        with closing(MySQLdb.connect(host=self.host, port=self.port,
                                     read_default_file="~/.my.cnf")) as conn:
            for dbName in self.removeDatabases:
                conn.cursor().execute(
                    "DROP DATABASE IF EXISTS {}".format(dbName))

    def testReRegistrationRaises(self):
        """A formattable object should only have one formatter registered for
        it, exactly one time. The AfwTableSqlFormatter for BaseCatalog self
        registeres, this is a second regisration and should raise a runtime
        error."""
        with self.assertRaises(RuntimeError):
            MySqlStorage.registerFormatter(
                lsst.afw.table.BaseCatalog, AfwTableSqlFormatter)

    def _getTestObject(self):
        """Test that Float columns can be written into the database storage and
        read back again.
        """
        schema = lsst.afw.table.Schema()
        aKey = schema.addField("a", type=float)
        bKey = schema.addField("b", type=float)
        outCat = lsst.afw.table.BaseCatalog(schema)
        record = outCat.addNew()
        record.set(aKey, 1.0)
        record.set(bKey, 1.5)
        return outCat

    def testIllegalDatabaseName(self):
        """Verify that bobby tables can't get in via a database name.

        Database creation does its own sanitization in mysqlStorage."""
        uri = "mysql://{}:{}/{}".format(
            self.host, self.port, "foo; DROP DATABASE bar")
        with self.assertRaises(RuntimeError):
            dafPersist.Storage.makeFromURI(uri)

    def testObjectWithoutFormatter(self):
        """Tests that a RuntimeError when a ButlerLocation specifying a
        pythonType that does not have an associated formatter registered with
        the storage is passed into storage.read or storage.write."""
        class TestClass:
            pass
        dbName = "TestMySqlStorage"
        self.removeDatabases.append(dbName)
        uri = "mysql://{}:{}/{}".format(self.host, self.port, dbName)
        self.assertIsNone(dafPersist.Storage.makeFromURI(uri, create=False))
        location = dafPersist.ButlerLocation(pythonType=TestClass,
                                             cppType=None,
                                             storageName=None,
                                             locationList="objectNameInStorage",
                                             dataId={},
                                             mapper={},
                                             storage={},
                                             usedDataId=None,
                                             datasetType=None)
        storage = dafPersist.Storage.makeFromURI(uri)
        obj = TestClass()
        with self.assertRaises(RuntimeError):
            storage.write(location, obj)
        with self.assertRaises(RuntimeError):
            storage.read(location)

    def testMySqlStorage(self):
        """Test mysqlStorage .write, .read, .copyFile, .locationWithRoot,
        .instanceSearch, .search"""
        dbName = "TestMySqlStorage"
        self.removeDatabases.append(dbName)
        uri = "mysql://{}:{}/{}".format(self.host, self.port, dbName)
        self.assertIsNone(dafPersist.Storage.makeFromURI(uri, create=False))
        location = dafPersist.ButlerLocation(pythonType=lsst.afw.table.BaseCatalog,
                                             cppType=None,
                                             storageName=None,
                                             locationList=["objectNameInStorage"],
                                             dataId={},
                                             mapper=None,
                                             storage=None,
                                             usedDataId=None,
                                             datasetType=None)
        storage = dafPersist.Storage.makeFromURI(uri)
        obj = self._getTestObject()
        storage.write(location, obj)
        reloadedObj = storage.read(location)
        self.assertEqual(obj.asAstropy(), reloadedObj.asAstropy())
        # test .exists:
        self.assertTrue(storage.exists(location))
        location.locationList = ['nonExistantObjectName']
        self.assertFalse(storage.exists(location))
        # test .copyFile:
        storage.copyFile('objectNameInStorage', 'nonExistantObjectName')
        self.assertTrue(storage.exists(location))
        reloadedObj = storage.read(location)
        self.assertEqual(obj.asAstropy(), reloadedObj.asAstropy())
        # test .locationWithRoot:
        self.assertEqual(os.path.join(uri, 'objectNameInStorage'),
                         storage.locationWithRoot('objectNameInStorage'))
        # test .instanceSearch:
        self.assertEqual('objectNameInStorage',
                         storage.instanceSearch('objectNameInStorage'))
        self.assertIsNone(storage.instanceSearch('nextNonExistingObjectName'))
        # test .search
        self.assertEqual('objectNameInStorage',
                         storage.search(uri, 'objectNameInStorage'))
        self.assertIsNone(storage.search(uri, 'nextNonExistingObjectName'))


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
